use graph::prelude::*;

use byte_slice_cast::ToByteSlice;
use log::info;

use std::path::PathBuf;
use std::time::Instant;

use super::*;

pub(crate) fn loading(args: CommonArgs, undirected: bool, weighted: bool) -> Result<()> {
    info!(
        "Reading graph ({} bit) from: {}",
        if args.use_32_bit { "32" } else { "64" },
        args.path.display()
    );

    run(args, undirected, weighted)?;

    Ok(())
}

fn run(args: CommonArgs, undirected: bool, weighted: bool) -> Result<()> {
    if weighted {
        run_node_idx::<usize>(args.use_32_bit, undirected, args.path)
    } else {
        run_node_idx::<()>(args.use_32_bit, undirected, args.path)
    }
}

fn run_node_idx<EV>(use_32_bit: bool, undirected: bool, path: PathBuf) -> Result<()>
where
    EV: ParseValue + std::fmt::Debug + Copy + Send + Sync,
{
    if use_32_bit {
        run_direction::<u32, EV>(undirected, path)
    } else {
        run_direction::<usize, EV>(undirected, path)
    }
}

fn run_direction<NI, EV>(undirected: bool, path: PathBuf) -> Result<()>
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

fn load<G, NI, EV>(path: PathBuf) -> Result<()>
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
