use graph::prelude::*;

use byte_slice_cast::ToByteSlice;
use log::info;

use std::convert::TryFrom;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::*;

pub(crate) fn serialize(args: CommonArgs, undirected: bool, output: PathBuf) -> Result<()> {
    let CommonArgs {
        path,
        format: _,
        runs: _,
        warmup_runs: _,
        use_32_bit,
    } = args;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    if undirected {
        if use_32_bit {
            run::<UndirectedCsrGraph<u32>, _>(path, output)?;
        } else {
            run::<UndirectedCsrGraph<usize>, _>(path, output)?;
        }
    } else if use_32_bit {
        run::<DirectedCsrGraph<u32>, _>(path, output)?;
    } else {
        run::<DirectedCsrGraph<usize>, _>(path, output)?;
    }

    Ok(())
}

fn run<G, NI>(path: PathBuf, output: PathBuf) -> Result<()>
where
    NI: Idx + ToByteSlice,
    G: Graph<NI>
        + From<(EdgeList<NI, ()>, CsrLayout)>
        + SerializeGraphOp<BufWriter<File>>
        + TryFrom<(PathBuf, CsrLayout), Error = Error>,
{
    let start = Instant::now();
    let actual = load_from_edge_list::<G, _>(path)?;
    info!("Loaded from edge list in {:?}.", start.elapsed());

    let start = Instant::now();
    serialize_into_binary::<G, _>(&actual, &output)?;
    info!("Serialized to binary in {:?}.", start.elapsed());

    let start = Instant::now();
    let expected = load_from_binary::<G, _>(output)?;
    info!("Loaded from binary in {:?}.", start.elapsed());

    assert_eq!(actual.node_count(), expected.node_count());
    assert_eq!(actual.edge_count(), expected.edge_count());

    Ok(())
}

fn load_from_edge_list<G, NI>(path: PathBuf) -> Result<G>
where
    NI: Idx + ToByteSlice,
    G: Graph<NI> + From<(EdgeList<NI, ()>, CsrLayout)>,
{
    let in_graph: G = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()
        .unwrap();

    Ok(in_graph)
}

fn serialize_into_binary<G, NI>(graph: &G, output: &Path) -> Result<()>
where
    NI: Idx + ToByteSlice,
    G: Graph<NI> + SerializeGraphOp<BufWriter<File>>,
{
    let writer = BufWriter::new(File::create(&output)?);
    G::serialize(graph, writer)?;
    Ok(())
}

fn load_from_binary<G, NI>(path: PathBuf) -> Result<G>
where
    NI: Idx + ToByteSlice,
    G: Graph<NI> + TryFrom<(PathBuf, CsrLayout)>,
    Error: From<<G as TryFrom<(PathBuf, CsrLayout)>>::Error>,
{
    let graph: G = GraphBuilder::new()
        .file_format(BinaryInput::<NI>::default())
        .path(path)
        .build()
        .unwrap();

    Ok(graph)
}
