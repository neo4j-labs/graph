use log::info;
use num_format::{Locale, ToFormattedString};
use std::{path::PathBuf, sync::atomic::Ordering, time::Instant};

use graph::prelude::*;

const CHUNK_SIZE: usize = 64;

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
        graph = relabel_graph(graph);
    }

    for _ in 0..runs {
        global_triangle_count(&graph);
    }

    Ok(())
}

fn relabel_graph<NI: Idx>(graph: UndirectedCsrGraph<NI>) -> UndirectedCsrGraph<NI> {
    let start = Instant::now();
    let graph = graph.to_degree_ordered();
    info!("Relabeled graph in {:?}", start.elapsed());
    graph
}

fn global_triangle_count<NI: Idx>(graph: &UndirectedCsrGraph<NI>) -> u64 {
    let start = Instant::now();

    let next_chunk = NI::zero().atomic();
    let total_triangles = AtomicU64::new(0);

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut triangles = 0;

                loop {
                    let start = next_chunk.fetch_add(NI::new(CHUNK_SIZE), Ordering::AcqRel);
                    if start >= graph.node_count() {
                        break;
                    }

                    let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

                    for u in start..end {
                        for &v in graph.neighbors(u) {
                            if v > u {
                                break;
                            }

                            let mut it = graph.neighbors(u);

                            for &w in graph.neighbors(v) {
                                if w > v {
                                    break;
                                }
                                while let Some(&x) = it.first() {
                                    if x >= w {
                                        if x == w {
                                            triangles += 1;
                                        }
                                        break;
                                    }
                                    it = &it[1..];
                                }
                            }
                        }
                    }
                }
                total_triangles.fetch_add(triangles, Ordering::AcqRel);
            });
        }
    });

    let tc = total_triangles.load(Ordering::SeqCst);

    info!(
        "Computed {} triangles in {:?}",
        tc.to_formatted_string(&Locale::en),
        start.elapsed()
    );

    tc
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CsrLayout;
    use crate::GraphBuilder;
    use crate::UndirectedCsrGraph;

    #[test]
    fn test_tc_two_components() {
        let gdl = "(a)-->()-->()<--(a),(b)-->()-->()<--(b)";

        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        assert_eq!(global_triangle_count(&graph), 2);
    }

    #[test]
    fn test_tc_connected_triangles() {
        let gdl = "(a)-->()-->()<--(a),(a)-->()-->()<--(a)";

        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        assert_eq!(global_triangle_count(&graph), 2);
    }

    #[test]
    fn test_tc_diamond() {
        let gdl = "(a)-->(b)-->(c)<--(a),(b)-->(d)<--(c)";

        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        assert_eq!(global_triangle_count(&graph), 2);
    }
}

use std::sync::atomic::AtomicU64;
