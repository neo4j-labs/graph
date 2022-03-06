use graph::prelude::*;

use clap::AppSettings::DeriveDisplayOrder;
use kommandozeile::*;

fn main() -> Result<()> {
    let args = setup_clap::<Args>().run()?;
    let filter_string = args.verbose.verbosity().as_filter_for_all();
    std::env::set_var("RUST_LOG", filter_string);
    env_logger::init();
        

    eprintln!("args: {args:#?}, filter: {filter_string}, path: {:?}", args.path.path());

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
    #[clap(short, long, parse(from_os_str))]
    path: InputFile,

    #[clap(short, long, arg_enum, default_value_t = FileFormat::EdgeList)]
    format: FileFormat,

    #[clap(long)]
    use_32_bit: bool,

    #[clap(short, long, default_value_t = 1)]
    runs: usize,

    #[clap(subcommand)]
    algorithm: Algorithm,

    #[clap(flatten)]
    verbose: Verbose,
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
