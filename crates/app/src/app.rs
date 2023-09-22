use std::{path::PathBuf, time::Instant};

use graph::prelude::*;

use crate::runner::runner;
use kommandozeile::*;
use log::info;

mod loading;
mod runner;
mod serialize;
mod sssp;
mod triangle_count;

runner!(page_rank, graph::page_rank::page_rank, PageRankConfig);
runner!(wcc, graph::wcc::wcc_afforest_dss, WccConfig);

fn main() -> Result<()> {
    let args = setup_clap::<Args>().run()?;
    let filter_string = args.verbose.verbosity().as_filter_for_all();
    std::env::set_var("RUST_LOG", filter_string);
    env_logger::init();

    match args.algorithm {
        Algorithm::PageRank { config } => page_rank::run(args.args, config)?,
        Algorithm::Sssp { config } => sssp::sssp(args.args, config)?,
        Algorithm::TriangleCount { relabel } => triangle_count::triangle_count(args.args, relabel)?,
        Algorithm::Wcc { config } => wcc::run(args.args, config)?,
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
#[clap(author, version, about, propagate_version = true)]
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
    #[clap(short, long, value_parser)]
    path: PathBuf,

    #[clap(short, long, value_enum, default_value_t = FileFormat::EdgeList)]
    format: FileFormat,

    #[clap(short, long, value_enum, default_value_t = GraphFormat::CompressedSparseRow)]
    graph: GraphFormat,

    #[clap(long)]
    use_32_bit: bool,

    #[clap(short, long, default_value_t = 1)]
    runs: usize,

    #[clap(short, long, default_value_t = 5)]
    warmup_runs: usize,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum GraphFormat {
    CompressedSparseRow,
    AdjacencyList,
}

#[derive(clap::ValueEnum, Debug, Clone)]
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
        #[clap(short, long, value_parser)]
        output: PathBuf,
        /// Load the graph as undirected.
        #[clap(long)]
        undirected: bool,
    },
}

pub(crate) fn time(runs: usize, warmup_runs: usize, f: impl Fn()) {
    for run in 1..=warmup_runs {
        let start = Instant::now();
        f();
        let took = start.elapsed();

        info!(
            "Warm-up run {} of {} finished in {:.6?}",
            run, warmup_runs, took,
        );
    }

    let mut durations = vec![];

    for run in 1..=runs {
        let start = Instant::now();
        f();
        let took = start.elapsed();
        durations.push(took);

        info!("Run {} of {} finished in {:.6?}", run, runs, took,);
    }

    let total = durations
        .into_iter()
        .reduce(|a, b| a + b)
        .unwrap_or_default();

    info!("Average runtime: {:?}", total / runs as u32);
}
