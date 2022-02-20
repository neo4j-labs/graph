use graph::prelude::*;

use log::info;

use std::collections::HashMap;
use std::hash::Hash;
use std::path::Path as StdPath;
use std::str::FromStr;
use std::time::Instant;

#[derive(Debug)]
enum FileFormat {
    EdgeList,
    Graph500,
}

impl FromStr for FileFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "edgelist" => Ok(Self::EdgeList),
            "graph500" => Ok(Self::Graph500),
            _ => Err(format!("unsupported file format {}", s)),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli::AppArgs {
        path,
        format,
        use_32_bit,
        runs,
        chunk_size,
        neighbor_rounds,
        sampling_size,
    } = cli::create()?;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    let config = WccConfig::new(chunk_size, neighbor_rounds, sampling_size);

    match (use_32_bit, format) {
        (true, FileFormat::EdgeList) => {
            run::<u32, _, _>(path, EdgeListInput::default(), runs, config)
        }
        (true, FileFormat::Graph500) => {
            run::<u32, _, _>(path, Graph500Input::default(), runs, config)
        }
        (false, FileFormat::EdgeList) => {
            run::<usize, _, _>(path, EdgeListInput::default(), runs, config)
        }
        (false, FileFormat::Graph500) => {
            run::<usize, _, _>(path, Graph500Input::default(), runs, config)
        }
    }
}

fn run<NI, Format, Path>(
    path: Path,
    file_format: Format,
    runs: usize,
    config: WccConfig,
) -> Result<(), Box<dyn std::error::Error>>
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

    for run in 1..=runs {
        let start = Instant::now();
        let dss = wcc_afforest_dss(&graph, config);
        info!(
            "Run {} of {} finished in {:.6?}",
            run,
            runs,
            start.elapsed(),
        );
        let mut components = HashMap::new();
        for node in 0..graph.node_count().index() {
            let component = dss.find(NI::new(node));
            let count = components.entry(component).or_insert(0);
            *count += 1;
        }
        info!("Found {} components.", components.len());
    }

    Ok(())
}

mod cli {
    use graph::wcc::WccConfig;
    use pico_args::Arguments;
    use std::{convert::Infallible, ffi::OsStr, path::PathBuf};

    #[derive(Debug)]
    pub(crate) struct AppArgs {
        pub(crate) path: std::path::PathBuf,
        pub(crate) format: crate::FileFormat,
        pub(crate) runs: usize,
        pub(crate) use_32_bit: bool,
        pub(crate) chunk_size: usize,
        pub(crate) neighbor_rounds: usize,
        pub(crate) sampling_size: usize,
    }

    pub(crate) fn create() -> Result<AppArgs, Box<dyn std::error::Error>> {
        let mut pargs = Arguments::from_env();

        fn as_path_buf(arg: &OsStr) -> Result<PathBuf, Infallible> {
            Ok(arg.into())
        }

        let default_config = WccConfig::default();

        let args = AppArgs {
            path: pargs.value_from_os_str(["-p", "--path"], as_path_buf)?,
            format: pargs
                .opt_value_from_str(["-f", "--format"])?
                .unwrap_or(crate::FileFormat::EdgeList),
            runs: pargs.opt_value_from_str(["-r", "--runs"])?.unwrap_or(1),
            use_32_bit: pargs.contains("--use-32-bit"),
            chunk_size: pargs
                .opt_value_from_str("--chunk-size")?
                .unwrap_or(default_config.chunk_size),
            neighbor_rounds: pargs
                .opt_value_from_str("--neighbor-rounds")?
                .unwrap_or(default_config.neighbor_rounds),
            sampling_size: pargs
                .opt_value_from_str("--sampling-size")?
                .unwrap_or(default_config.sampling_size),
        };

        Ok(args)
    }
}
