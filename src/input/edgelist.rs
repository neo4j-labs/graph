use log::info;
use std::{
    convert::TryFrom,
    fs::File,
    marker::PhantomData,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{index::Idx, Error};

use super::{EdgeList, InputCapabilities, InputPath};

pub struct EdgeListInput<Node: Idx> {
    _idx: PhantomData<Node>,
}

impl<Node: Idx> Default for EdgeListInput<Node> {
    fn default() -> Self {
        Self { _idx: PhantomData }
    }
}

impl<Node: Idx> InputCapabilities<Node> for EdgeListInput<Node> {
    type GraphInput = EdgeList<Node>;
}

impl<Node: Idx, P> TryFrom<InputPath<P>> for EdgeList<Node>
where
    P: AsRef<Path>,
{
    type Error = Error;

    fn try_from(path: InputPath<P>) -> Result<Self, Self::Error> {
        let file = File::open(path.0.as_ref())?;
        let mmap = unsafe { memmap2::MmapOptions::new().populate().map(&file)? };
        EdgeList::try_from(mmap.as_ref())
    }
}

impl<Node: Idx> TryFrom<&[u8]> for EdgeList<Node> {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let start = std::time::Instant::now();

        let page_size = page_size::get();
        let cpu_count = num_cpus::get_physical();
        let chunk_size =
            (usize::max(1, bytes.len() / cpu_count) + (page_size - 1)) & !(page_size - 1);

        info!(
            "page_size = {}, cpu_count = {}, chunk_size = {}",
            page_size, cpu_count, chunk_size
        );

        let all_edges = Arc::new(Mutex::new(Vec::new()));

        rayon::scope(|s| {
            for start in (0..bytes.len()).step_by(chunk_size) {
                let all_edges = Arc::clone(&all_edges);
                s.spawn(move |_| {
                    let mut end = usize::min(start + chunk_size, bytes.len());
                    while end <= bytes.len() && bytes[end - 1] != b'\n' {
                        end += 1;
                    }

                    let mut start = start;
                    if start != 0 {
                        while bytes[start - 1] != b'\n' {
                            start += 1;
                        }
                    }

                    let mut edges = Vec::new();
                    let mut chunk = &bytes[start..end];
                    while !chunk.is_empty() {
                        let (source, source_bytes) = Node::parse(chunk);
                        let (target, target_bytes) = Node::parse(&chunk[source_bytes + 1..]);
                        edges.push((source, target));
                        chunk = &chunk[source_bytes + target_bytes + 2..];
                    }

                    let mut all_edges = all_edges.lock().unwrap();
                    all_edges.append(&mut edges);
                });
            }
        });

        let edges = Arc::try_unwrap(all_edges).unwrap().into_inner().unwrap();

        let elapsed = start.elapsed().as_millis() as f64 / 1000_f64;

        info!(
            "Read {} edges in {:.2}s ({:.2} MB/s)",
            edges.len(),
            elapsed,
            ((bytes.len() as f64) / elapsed) / (1024.0 * 1024.0)
        );

        Ok(EdgeList::new(edges))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::input::InputPath;

    use super::*;

    #[test]
    fn edge_list_from_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let edge_list = EdgeList::<usize>::try_from(InputPath(path.as_path())).unwrap();

        assert_eq!(2, edge_list.max_node_id());
    }
}
