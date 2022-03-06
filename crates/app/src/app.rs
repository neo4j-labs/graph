use std::path::PathBuf;

use graph::prelude::*;

use clap::AppSettings::DeriveDisplayOrder;
use kommandozeile::*;

mod wcc;

fn main() -> Result<()> {
    let args = setup_clap::<Args>().run()?;
    let filter_string = args.verbose.verbosity().as_filter_for_all();
    std::env::set_var("RUST_LOG", filter_string);
    env_logger::init();

    match args.algorithm {
        Algorithm::PageRank {  } => todo!(),
        Algorithm::Sssp => todo!(),
        Algorithm::TriangleCount => todo!(),
        Algorithm::Wcc { config } => wcc::wcc(args.args, config)?,
    }

    Ok(())
}

#[derive(Debug, clap::Parser)]
#[clap(
    author, 
    version, 
    about, 
    propagate_version = true, 
    global_setting = DeriveDisplayOrder
)]
struct Args {

    #[clap(flatten)]
    args: CommonArgs,

    #[clap(subcommand)]
    algorithm: Algorithm,

    #[clap(flatten)]
    verbose: Verbose,
}

#[derive(Debug, clap::Args)]
struct CommonArgs {
    #[clap(short, long, parse(from_os_str))]
    path: PathBuf,

    #[clap(short, long, arg_enum, default_value_t = FileFormat::EdgeList)]
    format: FileFormat,

    #[clap(long)]
    use_32_bit: bool,

    #[clap(short, long, default_value_t = 1)]
    runs: usize,
}

#[derive(clap::ArgEnum, Debug, Clone)]
enum FileFormat {
    EdgeList,
    Graph500,
}

#[derive(clap::Subcommand, Debug)]
enum Algorithm {
    PageRank {},
    Sssp,
    TriangleCount,

    Wcc {
        #[clap(flatten)]
        config: WccConfig,
    },
}
