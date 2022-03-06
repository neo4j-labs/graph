use graph::prelude::*;

use log::info;
use std::path::Path as StdPath;

use super::*;

pub(crate) fn triangle_count(args: CommonArgs, relabel: bool) -> Result<()> {
    let CommonArgs {
        path,
        format,
        use_32_bit,
        runs,
    } = args;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    match (use_32_bit, format) {
        (true, FileFormat::EdgeList) => {
            run::<u32, _, _>(path, EdgeListInput::default(), runs, relabel)
        }
        (true, FileFormat::Graph500) => {
            run::<u32, _, _>(path, Graph500Input::default(), runs, relabel)
        }
        (false, FileFormat::EdgeList) => {
            run::<usize, _, _>(path, EdgeListInput::default(), runs, relabel)
        }
        (false, FileFormat::Graph500) => {
            run::<usize, _, _>(path, Graph500Input::default(), runs, relabel)
        }
    }
}

fn run<NI, Format, Path>(path: Path, file_format: Format, runs: usize, relabel: bool) -> Result<()>
where
    NI: Idx,
    Path: AsRef<StdPath>,
    Format: InputCapabilities<NI>,
    Format::GraphInput: TryFrom<InputPath<Path>>,
    UndirectedCsrGraph<NI>: TryFrom<(Format::GraphInput, CsrLayout)>,
    Error: From<<Format::GraphInput as TryFrom<InputPath<Path>>>::Error>,
    Error: From<<UndirectedCsrGraph<NI> as TryFrom<(Format::GraphInput, CsrLayout)>>::Error>,
{
    let mut graph: UndirectedCsrGraph<NI> = GraphBuilder::new()
        .csr_layout(CsrLayout::Deduplicated)
        .file_format(file_format)
        .path(path)
        .build()
        .unwrap();

    if relabel {
        relabel_graph(&mut graph);
    }

    for _ in 0..runs {
        global_triangle_count(&graph);
    }

    Ok(())
}
