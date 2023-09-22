use graph::prelude::*;

use log::info;

use super::*;

pub(crate) fn page_rank(args: CommonArgs, config: PageRankConfig) -> Result<()> {
    info!(
        "Reading graph ({} bit) from: {:?}",
        if args.use_32_bit { "32" } else { "64" },
        args.path
    );

    if args.use_32_bit {
        run::<u32>(args, config)
    } else {
        run::<u64>(args, config)
    }
}

fn run<NI: Idx>(args: CommonArgs, config: PageRankConfig) -> Result<()> {
    match args.format {
        FileFormat::EdgeList => {
            run_::<NI, EdgeListInput<NI>>(args, config, EdgeListInput::default())
        }
        FileFormat::Graph500 => {
            run_::<NI, Graph500Input<NI>>(args, config, Graph500Input::default())
        }
    }
}

fn run_<NI, Format>(args: CommonArgs, config: PageRankConfig, file_format: Format) -> Result<()>
where
    NI: Idx,
    Format: InputCapabilities<NI>,
    Format::GraphInput: TryFrom<InputPath<PathBuf>>,
    <Format as InputCapabilities<NI>>::GraphInput: Edges<NI = NI, EV = ()>,
    Error: From<<Format::GraphInput as TryFrom<InputPath<PathBuf>>>::Error>,
{
    match args.graph {
        GraphFormat::CompressedSparseRow => {
            run__::<DirectedCsrGraph<NI>, NI, Format>(args, config, file_format)
        }
        GraphFormat::AdjacencyList => {
            run__::<DirectedALGraph<NI>, NI, Format>(args, config, file_format)
        }
    }
}

fn run__<G, NI, Format>(args: CommonArgs, config: PageRankConfig, file_format: Format) -> Result<()>
where
    NI: Idx,
    Format: InputCapabilities<NI>,
    Format::GraphInput: TryFrom<InputPath<PathBuf>>,
    <Format as InputCapabilities<NI>>::GraphInput: Edges<NI = NI, EV = ()>,
    Error: From<<Format::GraphInput as TryFrom<InputPath<PathBuf>>>::Error>,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
    G: TryFrom<(Format::GraphInput, CsrLayout)>,
    Error: From<<G as TryFrom<(Format::GraphInput, CsrLayout)>>::Error>,
{
    let graph: G = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(file_format)
        .path(args.path)
        .build()?;

    time(args.runs, args.warmup_runs, || {
        graph::page_rank::page_rank(&graph, config);
    });

    Ok(())
}
