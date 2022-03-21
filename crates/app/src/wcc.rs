use graph::prelude::*;

use log::info;

use std::hash::Hash;
use std::path::Path as StdPath;
use std::time::Instant;

use super::*;

pub(crate) fn wcc(app_args: CommonArgs, config: WccConfig) -> Result<()> {
    let CommonArgs {
        path,
        format,
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
            run::<usize, _, _>(path, EdgeListInput::default(), runs, warmup_runs, config)
        }
        (false, FileFormat::Graph500) => {
            run::<usize, _, _>(path, Graph500Input::default(), runs, warmup_runs, config)
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

    for run in 1..=warmup_runs {
        let start = Instant::now();
        let _ = wcc_afforest_dss(&graph, config);
        let took = start.elapsed();

        info!(
            "Warm-up run {} of {} finished in {:.6?}",
            run, warmup_runs, took,
        );
    }

    let mut durations = vec![];

    for run in 1..=runs {
        let start = Instant::now();
        let _ = wcc_afforest_dss(&graph, config);
        let took = start.elapsed();
        durations.push(took);

        info!("Run {} of {} finished in {:.6?}", run, runs, took,);
    }

    let total = durations
        .into_iter()
        .reduce(|a, b| a + b)
        .unwrap_or_default();

    info!("Average runtime: {:?}", total / runs as u32);

    Ok(())
}
