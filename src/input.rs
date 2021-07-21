use rayon::prelude::*;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fs::File,
    io::Read,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{atomic::Ordering::SeqCst, Arc, Mutex},
};

use linereader::LineReader;

use crate::{index::AtomicIdx, index::Idx, Error, InputCapabilities};

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

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
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
                degrees[s.index()].fetch_add(1, SeqCst);
            }),
            Direction::Incoming => self.par_iter().for_each(|(_, t)| {
                degrees[t.index()].fetch_add(1, SeqCst);
            }),
            Direction::Undirected => self.par_iter().for_each(|(s, t)| {
                degrees[s.index()].fetch_add(1, SeqCst);
                degrees[t.index()].fetch_add(1, SeqCst);
            }),
        }

        // This is safe, since AtomicNode is guaranteed to have the same memory layout as Node.
        // unsafe { std::mem::transmute(degrees) }
        degrees
    }
}

pub struct MyPath<P>(pub(crate) P);

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

impl<Node: Idx, R> TryFrom<LineReader<R>> for EdgeList<Node>
where
    R: Read,
{
    type Error = Error;

    fn try_from(mut reader: LineReader<R>) -> Result<Self, Self::Error> {
        let mut edges = Vec::new();
        let mut bytes = 0_u64;
        let start = std::time::Instant::now();

        while let Some(lines) = reader.next_batch() {
            let mut lines = lines.expect("read error");
            bytes += lines.len() as u64;
            while !lines.is_empty() {
                let (source, pos) = Node::parse(lines);
                let (target, pos2) = Node::parse(&lines[pos + 1..]);
                edges.push((source, target));
                lines = &lines[pos + pos2 + 2..];
            }
        }

        let elapsed = start.elapsed().as_millis() as f64 / 1000_f64;

        println!(
            "Read {} edges in {:.2}s ({:.2} MB/s)",
            edges.len(),
            elapsed,
            ((bytes as f64) / elapsed) / (1024.0 * 1024.0)
        );

        Ok(EdgeList::new(edges))
    }
}

impl<Node: Idx> TryFrom<&[u8]> for EdgeList<Node> {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let start = std::time::Instant::now();

        let ps = dbg!(page_size::get());
        let cpus = dbg!(num_cpus::get_physical());
        let chunk_size =
            dbg!((dbg!(dbg!(usize::max(1, bytes.len() / cpus))) + (ps - 1)) & !(ps - 1));

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

        println!(
            "Read {} edges in {:.2}s ({:.2} MB/s)",
            edges.len(),
            elapsed,
            ((bytes.len() as f64) / elapsed) / (1024.0 * 1024.0)
        );

        Ok(EdgeList::new(edges))
    }
}

pub struct DotGraphInput<Node: Idx> {
    _idx: PhantomData<Node>,
}

impl<Node: Idx> Default for DotGraphInput<Node> {
    fn default() -> Self {
        Self { _idx: PhantomData }
    }
}

impl<Node: Idx> InputCapabilities<Node> for DotGraphInput<Node> {
    type GraphInput = DotGraph<Node>;
}

pub struct DotGraph<Node: Idx> {
    node_count: Node,
    relationship_count: Node,
    labels: Vec<usize>,
    offsets: Vec<Node>,
    neighbors: Vec<Node>,
    max_degree: Node,
    max_label: usize,
    label_frequency: HashMap<usize, usize>,
}

impl<Node: Idx, P> TryFrom<MyPath<P>> for DotGraph<Node>
where
    P: AsRef<Path>,
{
    type Error = Error;

    fn try_from(_: MyPath<P>) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

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
