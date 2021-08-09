use log::info;
use rayon::prelude::*;
use std::{
    convert::TryFrom,
    fs::File,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{atomic::Ordering::SeqCst, Arc, Mutex},
};

use crate::{index::AtomicIdx, index::Idx, Error};

use super::{Direction, InputCapabilities, MyPath};

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

pub struct EdgeList<Node: Idx>(Box<[(Node, Node)]>);

impl<Node: Idx> AsRef<[(Node, Node)]> for EdgeList<Node> {
    fn as_ref(&self) -> &[(Node, Node)] {
        &self.0
    }
}

impl<Node: Idx> Deref for EdgeList<Node> {
    type Target = [(Node, Node)];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Node: Idx> DerefMut for EdgeList<Node> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<Node: Idx> EdgeList<Node> {
    pub fn new(edges: Vec<(Node, Node)>) -> Self {
        Self(edges.into_boxed_slice())
    }

    pub(crate) fn max_node_id(&self) -> Node {
        self.0
            .par_iter()
            .map(|(s, t)| Node::max(*s, *t))
            .reduce(Node::zero, Node::max)
    }

    pub(crate) fn degrees(&self, node_count: Node, direction: Direction) -> Vec<Node::Atomic> {
        let mut degrees = Vec::with_capacity(node_count.index());
        degrees.resize_with(node_count.index(), Node::Atomic::zero);

        match direction {
            Direction::Outgoing => self.par_iter().for_each(|(s, _)| {
                degrees[s.index()].get_and_increment(SeqCst);
            }),
            Direction::Incoming => self.par_iter().for_each(|(_, t)| {
                degrees[t.index()].get_and_increment(SeqCst);
            }),
            Direction::Undirected => self.par_iter().for_each(|(s, t)| {
                degrees[s.index()].get_and_increment(SeqCst);
                degrees[t.index()].get_and_increment(SeqCst);
            }),
        }

        // This is safe, since AtomicNode is guaranteed to have the same memory layout as Node.
        // unsafe { std::mem::transmute(degrees) }
        degrees
    }
}

impl<Node: Idx, P> TryFrom<MyPath<P>> for EdgeList<Node>
where
    P: AsRef<Path>,
{
    type Error = Error;

    fn try_from(path: MyPath<P>) -> Result<Self, Self::Error> {
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

    use crate::input::MyPath;

    use super::*;

    #[test]
    fn edge_list_from_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let edge_list = EdgeList::<usize>::try_from(MyPath(path.as_path())).unwrap();

        assert_eq!(2, edge_list.max_node_id());
    }
}
