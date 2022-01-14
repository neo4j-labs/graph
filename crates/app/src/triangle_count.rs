use graph::prelude::*;

use log::info;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli::AppArgs {
        path,
        use_32_bit,
        runs,
        relabel,
    } = cli::create()?;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    if use_32_bit {
        run::<u32>(path, relabel, runs)
    } else {
        run::<usize>(path, relabel, runs)
    }
}

fn run<NI: Idx>(
    path: PathBuf,
    relabel: bool,
    runs: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut graph: UndirectedCsrGraph<NI> = GraphBuilder::new()
        .csr_layout(CsrLayout::Deduplicated)
        .file_format(EdgeListInput::default())
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
            runs: pargs.opt_value_from_str(["-r", "--runs"])?.unwrap_or(1),
            use_32_bit: pargs.contains("--use-32-bit"),
            relabel: pargs.contains("--relabel"),
        };

        Ok(args)
    }
}
