use byte_slice_cast::ToByteSlice;
use graph::input::EdgeList;
use log::info;

use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use graph::prelude::*;

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
            run::<UndirectedCsrGraph<u32>, _>(path, output)
        } else {
            run::<UndirectedCsrGraph<usize>, _>(path, output)
        }
    } else {
        if use_32_bit {
            run::<DirectedCsrGraph<u32>, _>(path, output)
        } else {
            run::<DirectedCsrGraph<usize>, _>(path, output)
        }
    }
}

fn run<G, Node>(path: PathBuf, output: PathBuf) -> Result<(), Box<dyn std::error::Error>>
where
    Node: Idx + ToByteSlice,
    G: Graph<Node>
        + From<(EdgeList<Node>, CsrLayout)>
        + SerializeGraphOp<File>
        + DeserializeGraphOp<File, G>,
{
    let in_graph: G = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()
        .unwrap();

    let start = Instant::now();
    let file = File::create(&output)?;
    G::serialize(&in_graph, file)?;
    info!("Serialized graph in {:?}", start.elapsed());

    let start = Instant::now();
    let file = File::open(&output)?;
    let out_graph = G::deserialize(file).unwrap();
    info!("Deserialized graph in {:?}", start.elapsed());

    assert_eq!(in_graph.node_count(), out_graph.node_count());
    assert_eq!(in_graph.edge_count(), out_graph.edge_count());

    Ok(())
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
