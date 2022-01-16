use graph::prelude::*;

use log::info;

use std::path::Path as StdPath;
use std::str::FromStr;

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
        relabel,
    } = cli::create()?;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    match (use_32_bit, format) {
        (true, FileFormat::EdgeList) => {
            run::<u32, _, _>(path, EdgeListInput::default(), relabel, runs)
        }
        (true, FileFormat::Graph500) => {
            run::<u32, _, _>(path, Graph500Input::default(), relabel, runs)
        }
        (false, FileFormat::EdgeList) => {
            run::<usize, _, _>(path, Graph500Input::default(), relabel, runs)
        }
        (false, FileFormat::Graph500) => {
            run::<usize, _, _>(path, Graph500Input::default(), relabel, runs)
        }
    }
}

fn run<NI, Format, Path>(
    path: Path,
    file_format: Format,
    relabel: bool,
    runs: usize,
) -> Result<(), Box<dyn std::error::Error>>
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

mod cli {
    use pico_args::Arguments;
    use std::{convert::Infallible, ffi::OsStr, path::PathBuf};

    #[derive(Debug)]
    pub(crate) struct AppArgs {
        pub(crate) path: std::path::PathBuf,
        pub(crate) format: crate::FileFormat,
        pub(crate) runs: usize,
        pub(crate) use_32_bit: bool,
        pub(crate) relabel: bool,
    }

    pub(crate) fn create() -> Result<AppArgs, Box<dyn std::error::Error>> {
        let mut pargs = Arguments::from_env();

        fn as_path_buf(arg: &OsStr) -> Result<PathBuf, Infallible> {
            Ok(arg.into())
        }

        let args = AppArgs {
            path: pargs.value_from_os_str(["-p", "--path"], as_path_buf)?,
            format: pargs
                .opt_value_from_str(["-f", "--format"])?
                .unwrap_or(crate::FileFormat::EdgeList),
            runs: pargs.opt_value_from_str(["-r", "--runs"])?.unwrap_or(1),
            use_32_bit: pargs.contains("--use-32-bit"),
            relabel: pargs.contains("--relabel"),
        };

        Ok(args)
    }
}
