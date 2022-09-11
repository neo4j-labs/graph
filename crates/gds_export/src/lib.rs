use std::sync::Arc;

use arrow::{
    array::PrimitiveArray,
    datatypes::{DataType, Field, Int64Type, Schema, SchemaRef},
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_from_arrow_batch, FlightData,
    FlightDescriptor, SchemaAsIpc,
};
use futures::{
    channel::mpsc,
    stream::{self, StreamExt},
    SinkExt,
};
use graph_builder::prelude::*;
use http::Uri;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tonic::Request;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;
type Client = FlightServiceClient<tonic::transport::Channel>;

const CHUNK_SIZE: usize = 10_000;

#[derive(Serialize)]
pub struct ExportConfig {
    #[serde(skip_serializing)]
    uri: Uri,
    name: String,
    database_name: String,
    concurrency: u32,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            uri: "grpc+tcp://localhost:8491".parse::<Uri>().unwrap(),
            name: "graph".to_string(),
            database_name: "neo4j".to_string(),
            concurrency: 4,
        }
    }
}

#[derive(Deserialize)]
struct PutStatusUpdate {
    imported: usize,
}

pub async fn export<NI, G>(g: &G, config: ExportConfig) -> Result<()>
where
    NI: Idx,
    G: Graph<NI> + DirectedNeighbors<NI>,
{
    let mut client = Client::connect(config.uri.clone()).await?;

    // init graph creation
    let action = arrow_flight::Action {
        r#type: "CREATE_GRAPH".to_string(),
        body: serde_json::to_string(&config)?.into_bytes(),
    };
    let _ = client.do_action(Request::new(action)).await?;

    send_nodes(g, &mut client, &config).await?;

    // nodes done action
    let action = arrow_flight::Action {
        r#type: "NODE_LOAD_DONE".to_string(),
        body: json!({"name": config.name}).to_string().into_bytes(),
    };
    let _ = client.do_action(Request::new(action)).await?;

    send_edges(g, &mut client, &config).await?;

    // edges done action
    let action = arrow_flight::Action {
        r#type: "RELATIONSHIP_LOAD_DONE".to_string(),
        body: json!({"name": config.name}).to_string().into_bytes(),
    };
    let _ = client.do_action(Request::new(action)).await?;

    Ok(())
}

async fn send_nodes<NI, G>(g: &G, client: &mut Client, config: &ExportConfig) -> Result<()>
where
    NI: Idx,
    G: Graph<NI>,
{
    let descriptor = FlightDescriptor::new_cmd(
        json!({ "name": &config.name, "entity_type": "node" })
            .to_string()
            .into_bytes(),
    );
    let field = Field::new("nodeId", DataType::Int64, false);
    let schema = Schema::new(vec![field]);
    let schema = Arc::new(schema);

    let chunks = (0..g.node_count().index() as i64).chunks(CHUNK_SIZE);

    let batches = chunks.into_iter().map(|node_chunk| {
        let node_chunk = PrimitiveArray::<Int64Type>::from_iter_values(node_chunk);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(node_chunk)]).unwrap()
    });

    let rows_imported = upload_batches(client, schema.clone(), descriptor, batches).await?;

    assert_eq!(rows_imported, g.node_count().index());

    Ok(())
}

async fn send_edges<NI, G>(g: &G, client: &mut Client, config: &ExportConfig) -> Result<()>
where
    NI: Idx,
    G: Graph<NI> + DirectedNeighbors<NI>,
{
    let descriptor = FlightDescriptor::new_cmd(
        json!({ "name": &config.name, "entity_type": "relationship" })
            .to_string()
            .into_bytes(),
    );
    let source_id_field = Field::new("sourceNodeId", DataType::Int64, false);
    let target_id_field = Field::new("targetNodeId", DataType::Int64, false);
    let schema = Schema::new(vec![source_id_field, target_id_field]);
    let schema = Arc::new(schema);

    let chunks = (0..g.node_count().index())
        .flat_map(|source| {
            g.out_neighbors(Idx::new(source))
                .map(move |target| (source, target.index()))
        })
        .map(|(s, t)| (s as i64, t as i64))
        .chunks(CHUNK_SIZE);

    let batches = chunks.into_iter().map(|edge_chunk| {
        let (sources, targets): (Vec<_>, Vec<_>) = edge_chunk.unzip();

        let sources = PrimitiveArray::<Int64Type>::from_iter_values(sources);
        let targets = PrimitiveArray::<Int64Type>::from_iter_values(targets);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(sources), Arc::new(targets)]).unwrap()
    });

    let rows_imported = upload_batches(client, schema.clone(), descriptor, batches).await?;

    assert_eq!(rows_imported, g.edge_count().index());

    Ok(())
}

async fn upload_batches<I>(
    client: &mut Client,
    schema: SchemaRef,
    descriptor: FlightDescriptor,
    mut batches: I,
) -> Result<usize>
where
    I: Iterator<Item = RecordBatch>,
{
    let (mut upload_tx, upload_rx) = mpsc::channel(10);

    let ipc_write_options = IpcWriteOptions::default();
    let schema_ipc = SchemaAsIpc::new(&schema, &ipc_write_options);
    let mut schema_flight_data = FlightData::from(schema_ipc);
    schema_flight_data.flight_descriptor = Some(descriptor.clone());
    upload_tx.send(schema_flight_data).await?;

    let mut rows_imported = 0;

    if let Some(first_batch) = batches.next() {
        // load first batch into channel before starting the request
        send_batch(&first_batch, &ipc_write_options, &mut upload_tx).await?;

        let mut put_results = client.do_put(Request::new(upload_rx)).await?.into_inner();

        let res = put_results
            .next()
            .await
            .expect("No response received")
            .expect("Invalid response received");

        let res = serde_json::from_slice::<PutStatusUpdate>(&res.app_metadata)?;

        assert_eq!(res.imported - rows_imported, first_batch.num_rows());
        rows_imported = res.imported;

        // stream the remaining batches
        for batch in batches {
            send_batch(&batch, &ipc_write_options, &mut upload_tx).await?;

            let res = put_results
                .next()
                .await
                .expect("No response received")
                .expect("Invalid response received");

            let res = serde_json::from_slice::<PutStatusUpdate>(&res.app_metadata)?;
            assert_eq!(res.imported - rows_imported, batch.num_rows());
            rows_imported = res.imported;
        }
        drop(upload_tx);
        assert!(
            put_results.next().await.is_none(),
            "Should not receive more results"
        );
    } else {
        drop(upload_tx);
        client.do_put(Request::new(upload_rx)).await?;
    }

    Ok(rows_imported)
}

async fn send_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
    upload_tx: &mut mpsc::Sender<FlightData>,
) -> Result<()> {
    let (dict_flight_data, batch_flight_data) = flight_data_from_arrow_batch(batch, options);

    upload_tx
        .send_all(&mut stream::iter(dict_flight_data).map(Ok))
        .await?;

    upload_tx.send(batch_flight_data).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_graph() {
        let gdl = "(a)-->()-->()<--(a),(b)-->()-->()<--(b)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let config = ExportConfig {
            name: "my_graph".to_string(),
            ..ExportConfig::default()
        };

        let res = export(&graph, config).await;

        match res {
            Ok(_) => (),
            Err(e) => println!("{e}"),
        };
    }
}
