use byte_slice_cast::ToByteSlice;
use graph::graph_ops::SerializeGraphOp;
use graph::prelude::*;
use log::info;

use std::convert::TryFrom;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli::AppArgs {
        path,
        use_32_bit,
        undirected,
        output,
    } = cli::create()?;

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

fn run<G, NI>(path: PathBuf, output: PathBuf) -> Result<(), Error>
where
    NI: Idx + ToByteSlice,
    G: Graph<NI>
        + From<(EdgeList<NI, ()>, CsrLayout)>
        + SerializeGraphOp<BufWriter<File>>
        + TryFrom<(PathBuf, CsrLayout), Error = graph::Error>,
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

fn load_from_edge_list<G, NI>(path: PathBuf) -> Result<G, Error>
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

fn serialize_into_binary<G, NI>(graph: &G, output: &Path) -> Result<(), Error>
where
    NI: Idx + ToByteSlice,
    G: Graph<NI> + SerializeGraphOp<BufWriter<File>>,
{
    let writer = BufWriter::new(File::create(&output)?);
    G::serialize(graph, writer)?;
    Ok(())
}

fn load_from_binary<G, NI>(path: PathBuf) -> Result<G, Error>
where
    NI: Idx + ToByteSlice,
    G: Graph<NI> + TryFrom<(PathBuf, CsrLayout)>,
    graph::Error: From<<G as TryFrom<(PathBuf, CsrLayout)>>::Error>,
{
    let graph: G = GraphBuilder::new()
        .file_format(BinaryInput::<NI>::default())
        .path(path)
        .build()
        .unwrap();

    Ok(graph)
}

mod cli {
    use pico_args::Arguments;
    use std::{convert::Infallible, ffi::OsStr, path::PathBuf};

    #[derive(Debug)]
    pub(crate) struct AppArgs {
        pub(crate) path: std::path::PathBuf,
        pub(crate) use_32_bit: bool,
        pub(crate) undirected: bool,
        pub(crate) output: std::path::PathBuf,
    }

    pub(crate) fn create() -> Result<AppArgs, Box<dyn std::error::Error>> {
        let mut pargs = Arguments::from_env();

        fn as_path_buf(arg: &OsStr) -> Result<PathBuf, Infallible> {
            Ok(arg.into())
        }

        let args = AppArgs {
            path: pargs.value_from_os_str(["-p", "--path"], as_path_buf)?,
            use_32_bit: pargs.contains("--use-32-bit"),
            undirected: pargs.contains("--undirected"),
            output: pargs.value_from_os_str(["-o", "--output"], as_path_buf)?,
        };

        Ok(args)
    }
}
