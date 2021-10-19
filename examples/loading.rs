use byte_slice_cast::ToByteSlice;
use graph::prelude::*;
use log::info;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = cli::create()?;

    info!(
        "Reading graph ({} bit) from: {}",
        if args.use_32_bit { "32" } else { "64" },
        args.path.display()
    );

    run(args)?;
    Ok(())
}

fn run(args: cli::AppArgs) -> Result<(), Error> {
    if args.weighted_input {
        run_node_idx::<usize>(args.use_32_bit, args.undirected, args.path)
    } else {
        run_node_idx::<()>(args.use_32_bit, args.undirected, args.path)
    }
}

fn run_node_idx<EV>(use_32_bit: bool, undirected: bool, path: PathBuf) -> Result<(), Error>
where
    EV: ParseValue + std::fmt::Debug + Copy + Send + Sync,
{
    if use_32_bit {
        run_direction::<u32, EV>(undirected, path)
    } else {
        run_direction::<usize, EV>(undirected, path)
    }
}

fn run_direction<NI, EV>(undirected: bool, path: PathBuf) -> Result<(), Error>
where
    NI: Idx + ToByteSlice,
    EV: ParseValue + std::fmt::Debug + Copy + Send + Sync,
{
    if undirected {
        load::<UndirectedCsrGraph<NI, (), EV>, NI, EV>(path)
    } else {
        load::<DirectedCsrGraph<NI, (), EV>, NI, EV>(path)
    }
}

fn load<G, NI, EV>(path: PathBuf) -> Result<(), Error>
where
    NI: Idx + ToByteSlice,
    EV: ParseValue + std::fmt::Debug + Send + Sync,
    G: Graph<NI> + From<(EdgeList<NI, EV>, CsrLayout)>,
{
    let start = Instant::now();
    let graph: G = GraphBuilder::new()
        .csr_layout(CsrLayout::Unsorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()?;

    info!(
        "Loaded {:?} nodes, {:?} edges from edge list in {:?}.",
        graph.node_count(),
        graph.edge_count(),
        start.elapsed()
    );

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
        pub(crate) weighted_input: bool,
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
            weighted_input: pargs.contains("--weighted"),
        };

        Ok(args)
    }
}
