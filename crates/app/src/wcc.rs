use graph::prelude::*;

use log::info;

use std::hash::Hash;
use std::path::Path as StdPath;

use super::*;

pub(crate) fn wcc(app_args: CommonArgs, config: WccConfig) -> Result<()> {
    let CommonArgs {
        path,
        format,
        graph: _,
        use_32_bit,
        runs,
        warmup_runs,
    } = app_args;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    match (use_32_bit, format) {
        (true, FileFormat::EdgeList) => {
            run::<u32, _, _>(path, EdgeListInput::default(), runs, warmup_runs, config)
        }
        (true, FileFormat::Graph500) => {
            run::<u32, _, _>(path, Graph500Input::default(), runs, warmup_runs, config)
        }
        (false, FileFormat::EdgeList) => {
            run::<u64, _, _>(path, EdgeListInput::default(), runs, warmup_runs, config)
        }
        (false, FileFormat::Graph500) => {
            run::<u64, _, _>(path, Graph500Input::default(), runs, warmup_runs, config)
        }
    }
}

fn run<NI, Format, Path>(
    path: Path,
    file_format: Format,
    runs: usize,
    warmup_runs: usize,
    config: WccConfig,
) -> Result<()>
where
    NI: Idx + Hash,
    Path: AsRef<StdPath>,
    Format: InputCapabilities<NI>,
    Format::GraphInput: TryFrom<InputPath<Path>>,
    DirectedCsrGraph<NI>: TryFrom<(Format::GraphInput, CsrLayout)>,
    Error: From<<Format::GraphInput as TryFrom<InputPath<Path>>>::Error>,
    Error: From<<DirectedCsrGraph<NI> as TryFrom<(Format::GraphInput, CsrLayout)>>::Error>,
{
    let graph: DirectedCsrGraph<NI> = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(file_format)
        .path(path)
        .build()?;

    time(runs, warmup_runs, || {
        wcc_afforest_dss(&graph, config);
    });

    Ok(())
}
