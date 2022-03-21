use std::path::PathBuf;

use graph::prelude::*;

use clap::AppSettings::DeriveDisplayOrder;
use kommandozeile::*;

mod loading;
mod page_rank;
mod serialize;
mod sssp;
mod triangle_count;
mod wcc;

fn main() -> Result<()> {
    let args = setup_clap::<Args>().run()?;
    let filter_string = args.verbose.verbosity().as_filter_for_all();
    std::env::set_var("RUST_LOG", filter_string);
    env_logger::init();

    match args.algorithm {
        Algorithm::PageRank { config } => page_rank::page_rank(args.args, config)?,
        Algorithm::Sssp { config } => sssp::sssp(args.args, config)?,
        Algorithm::TriangleCount { relabel } => triangle_count::triangle_count(args.args, relabel)?,
        Algorithm::Wcc { config } => wcc::wcc(args.args, config)?,
        Algorithm::Loading {
            undirected,
            weighted,
        } => loading::loading(args.args, undirected, weighted)?,
        Algorithm::Serialize { output, undirected } => {
            serialize::serialize(args.args, undirected, output)?
        }
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
    verbose: Verbose<Global>,
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

    #[clap(short, long, default_value_t = 5)]
    warmup_runs: usize,
}

#[derive(clap::ArgEnum, Debug, Clone)]
enum FileFormat {
    EdgeList,
    Graph500,
}

#[derive(clap::Subcommand, Debug)]
enum Algorithm {
    PageRank {
        #[clap(flatten)]
        config: PageRankConfig,
    },
    Sssp {
        #[clap(flatten)]
        config: DeltaSteppingConfig,
    },
    TriangleCount {
        #[clap(long)]
        relabel: bool,
    },

    Wcc {
        #[clap(flatten)]
        config: WccConfig,
    },
    Loading {
        /// Load the graph as undirected.
        #[clap(long)]
        undirected: bool,
        /// Load the graph as weighted.
        #[clap(long)]
        weighted: bool,
    },
    Serialize {
        /// Path to serialize graph to.
        #[clap(short, long, parse(from_os_str))]
        output: PathBuf,
        /// Load the graph as undirected.
        #[clap(long)]
        undirected: bool,
    },
}
