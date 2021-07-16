use atoi::FromRadix10;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fs::File,
    io::Read,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
};

use linereader::LineReader;

use crate::InputCapabilities;

pub struct EdgeListInput;

impl InputCapabilities for EdgeListInput {
    type GraphInput = EdgeList;
}

pub type Edge = (usize, usize);

pub struct EdgeList(Box<[Edge]>);

impl AsRef<[Edge]> for EdgeList {
    fn as_ref(&self) -> &[Edge] {
        &self.0
    }
}

impl Deref for EdgeList {
    type Target = [Edge];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EdgeList {
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

impl EdgeList {
    pub fn new(edges: Vec<Edge>) -> Self {
        Self(edges.into_boxed_slice())
    }

    pub(crate) fn max_node_id(&self) -> usize {
        self.0
            .par_iter()
            .map(|(s, t)| usize::max(*s, *t))
            .reduce(|| 0_usize, usize::max)
    }

    pub(crate) fn degrees(&self, node_count: usize, direction: Direction) -> Vec<AtomicUsize> {
        let mut degrees = Vec::with_capacity(node_count);
        degrees.resize_with(node_count, || AtomicUsize::new(0));

        match direction {
            Direction::Outgoing => self.par_iter().for_each(|(s, _)| {
                degrees[*s].fetch_add(1, SeqCst);
            }),
            Direction::Incoming => self.par_iter().for_each(|(_, t)| {
                degrees[*t].fetch_add(1, SeqCst);
            }),
            Direction::Undirected => self.par_iter().for_each(|(s, t)| {
                degrees[*s].fetch_add(1, SeqCst);
                degrees[*t].fetch_add(1, SeqCst);
            }),
        }

        // This is safe, since AtomicUsize is guaranteed to have the same memory layout as usize.
        // unsafe { std::mem::transmute(degrees) }
        degrees
    }
}

impl From<&Path> for EdgeList {
    fn from(path: &Path) -> Self {
        let file = File::open(path).unwrap();
        // let reader = LineReader::new(file);
        let mmap = unsafe { memmap2::MmapOptions::new().populate().map(&file).unwrap() };
        EdgeList::try_from(mmap.as_ref()).unwrap()
    }
}

impl<R> TryFrom<LineReader<R>> for EdgeList
where
    R: Read,
{
    type Error = std::io::Error;

    fn try_from(mut reader: LineReader<R>) -> Result<Self, Self::Error> {
        let mut edges = Vec::new();
        let mut bytes = 0_u64;
        let start = std::time::Instant::now();

        while let Some(lines) = reader.next_batch() {
            let mut lines = lines.expect("read error");
            bytes += lines.len() as u64;
            while !lines.is_empty() {
                let (source, pos) = usize::from_radix_10(lines);
                let (target, pos2) = usize::from_radix_10(&lines[pos + 1..]);
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

impl TryFrom<&[u8]> for EdgeList {
    type Error = std::io::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let start = std::time::Instant::now();

        let ps = dbg!(page_size::get());
        let cpus = dbg!(num_cpus::get_physical());
        let chunk_size = dbg!((dbg!(dbg!(bytes.len()) / cpus) + (ps - 1)) & !(ps - 1));

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
                        let (source, source_bytes) = usize::from_radix_10(chunk);
                        let (target, target_bytes) =
                            usize::from_radix_10(&chunk[source_bytes + 1..]);
                        edges.push((source, target));
                        chunk = &chunk[source_bytes + target_bytes + 2..];
                    }

                    let mut all_edges = all_edges.lock().unwrap();
                    all_edges.append(&mut edges);
                });
            }
        });

        let edges = Arc::try_unwrap(all_edges).unwrap().into_inner().unwrap();

        // let elapsed = start.elapsed().as_millis() as f64 / 1000_f64;

        // println!(
        //     "Read {} edges in {:.2}s ({:.2} MB/s)",
        //     all_edges.len(),
        //     elapsed,
        //     ((bytes.len() as f64) / elapsed) / (1024.0 * 1024.0)
        // );

        // let edges = all_edges
        //     .into_iter()
        //     .flat_map(|e| e.into_iter())
        //     .collect::<Vec<_>>();

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

pub struct DotGraphInput;

impl InputCapabilities for DotGraphInput {
    type GraphInput = DotGraph;
}

pub struct DotGraph {
    node_count: usize,
    relationship_count: usize,
    labels: Vec<usize>,
    offsets: Vec<usize>,
    neighbors: Vec<usize>,
    max_degree: usize,
    max_label: usize,
    label_frequency: HashMap<usize, usize>,
}

impl From<&Path> for DotGraph {
    fn from(_: &Path) -> Self {
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

        let edge_list = EdgeList::from(path.as_path());

        assert_eq!(2, edge_list.max_node_id());
    }
}
