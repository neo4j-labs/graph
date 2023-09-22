use graph::prelude::*;

use log::info;

use std::path::Path as StdPath;

use super::*;

pub(crate) fn page_rank(args: CommonArgs, config: PageRankConfig) -> Result<()> {
    let CommonArgs {
        path,
        format,
        graph,
        use_32_bit,
        runs,
        warmup_runs,
    } = args;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    match (graph, use_32_bit, format) {
        (GraphFormat::CompressedSparseRow, true, FileFormat::EdgeList) => {
            run::<DirectedCsrGraph<u32>, u32, _, _>(
                path,
                EdgeListInput::default(),
                runs,
                warmup_runs,
                config,
            )
        }
        (GraphFormat::CompressedSparseRow, true, FileFormat::Graph500) => {
            run::<DirectedCsrGraph<u32>, u32, _, _>(
                path,
                Graph500Input::default(),
                runs,
                warmup_runs,
                config,
            )
        }
        (GraphFormat::CompressedSparseRow, false, FileFormat::EdgeList) => {
            run::<DirectedCsrGraph<u64>, u64, _, _>(
                path,
                EdgeListInput::default(),
                runs,
                warmup_runs,
                config,
            )
        }
        (GraphFormat::CompressedSparseRow, false, FileFormat::Graph500) => {
            run::<DirectedCsrGraph<u64>, u64, _, _>(
                path,
                Graph500Input::default(),
                runs,
                warmup_runs,
                config,
            )
        }
        (GraphFormat::AdjacencyList, true, FileFormat::EdgeList) => {
            run::<DirectedALGraph<u32>, u32, _, _>(
                path,
                EdgeListInput::default(),
                runs,
                warmup_runs,
                config,
            )
        }
        (GraphFormat::AdjacencyList, true, FileFormat::Graph500) => {
            run::<DirectedALGraph<u32>, u32, _, _>(
                path,
                Graph500Input::default(),
                runs,
                warmup_runs,
                config,
            )
        }
        (GraphFormat::AdjacencyList, false, FileFormat::EdgeList) => {
            run::<DirectedALGraph<u64>, u64, _, _>(
                path,
                EdgeListInput::default(),
                runs,
                warmup_runs,
                config,
            )
        }
        (GraphFormat::AdjacencyList, false, FileFormat::Graph500) => {
            run::<DirectedALGraph<u64>, u64, _, _>(
                path,
                Graph500Input::default(),
                runs,
                warmup_runs,
                config,
            )
        }
    }
}

fn run<G, NI, Format, Path>(
    path: Path,
    file_format: Format,
    runs: usize,
    warmup_runs: usize,
    config: PageRankConfig,
) -> Result<()>
where
    NI: Idx,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
    Path: AsRef<StdPath>,
    Format: InputCapabilities<NI>,
    Format::GraphInput: TryFrom<InputPath<Path>>,
    G: TryFrom<(Format::GraphInput, CsrLayout)>,
    Error: From<<Format::GraphInput as TryFrom<InputPath<Path>>>::Error>,
    Error: From<<G as TryFrom<(Format::GraphInput, CsrLayout)>>::Error>,
{
    let graph: G = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(file_format)
        .path(path)
        .build()?;

    time(runs, warmup_runs, || {
        graph::page_rank::page_rank(&graph, config);
    });

    Ok(())
}
