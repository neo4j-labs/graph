use log::info;
use std::{path::PathBuf, sync::atomic::Ordering, time::Instant};

use graph::prelude::*;

use graph::index::AtomicIdx;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli::AppArgs {
        path,
        use_32_bit,
        iterations,
        relabel,
    } = cli::create()?;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    if use_32_bit {
        run::<u32>(path, relabel, iterations)
    } else {
        run::<usize>(path, relabel, iterations)
    }
}

fn run<Node: Idx>(
    path: PathBuf,
    relabel: bool,
    iterations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut g: UndirectedCSRGraph<Node> = GraphBuilder::new()
        .csr_option(CSROption::Deduplicated)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()
        .unwrap();

    if relabel {
        g = relabel_graph(g);
    }

    for _ in 0..iterations {
        global_triangle_count(&g);
    }

    Ok(())
}

fn relabel_graph<Node: Idx>(graph: UndirectedCSRGraph<Node>) -> UndirectedCSRGraph<Node> {
    let start = Instant::now();
    let graph = graph.relabel_by_degree();
    info!("relabel_by_degree() took {:?}", start.elapsed());
    graph
}

fn global_triangle_count<Node: Idx>(graph: &UndirectedCSRGraph<Node>) -> u64 {
    let start = Instant::now();

    let next_chunk = Node::zero().atomic();
    let total_triangles = AtomicU64::new(0);

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut triangles = 0;

                loop {
                    let start = next_chunk.fetch_add(Node::new(64), Ordering::AcqRel);
                    if start >= graph.node_count() {
                        break;
                    }

                    let end = (start + Node::new(64)).min(graph.node_count());

                    for n in start.index()..end.index() {
                        let u = Node::new(n);

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
        "Triangle counting finished in {:?} seconds .. global triangle count = {}",
        start.elapsed(),
        tc
    );

    tc
}

mod cli {
    use pico_args::Arguments;
    use std::{convert::Infallible, ffi::OsStr, path::PathBuf};

    #[derive(Debug)]
    pub(crate) struct AppArgs {
        pub(crate) path: std::path::PathBuf,
        pub(crate) iterations: usize,
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
            iterations: pargs
                .opt_value_from_str(["-i", "--iterations"])?
                .unwrap_or(1),
            use_32_bit: pargs.contains("--use-32-bit"),
            relabel: pargs.contains("--relabel"),
        };

        Ok(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CSROption;
    use crate::GraphBuilder;
    use crate::UndirectedCSRGraph;

    #[test]
    fn test_tc_two_components() {
        let edges = vec![(0, 1), (0, 2), (1, 2), (3, 4), (3, 5), (4, 5)];

        let g: UndirectedCSRGraph<usize> = GraphBuilder::new()
            .csr_option(CSROption::Deduplicated)
            .edges(edges)
            .build();

        assert_eq!(global_triangle_count(&g), 2);
    }

    #[test]
    fn test_tc_connected_triangles() {
        let edges = vec![(0, 1), (0, 2), (1, 2), (0, 4), (0, 5), (4, 5)];

        let g: UndirectedCSRGraph<usize> = GraphBuilder::new()
            .csr_option(CSROption::Deduplicated)
            .edges(edges)
            .build();

        assert_eq!(global_triangle_count(&g), 2);
    }

    #[test]
    fn test_tc_diamond() {
        let edges = vec![(0, 1), (0, 2), (1, 2), (1, 4), (2, 4)];

        let g: UndirectedCSRGraph<usize> = GraphBuilder::new()
            .csr_option(CSROption::Deduplicated)
            .edges(edges)
            .build();

        assert_eq!(global_triangle_count(&g), 2);
    }
}

use std::sync::atomic::AtomicU64;
