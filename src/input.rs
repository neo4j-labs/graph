use atoi::FromRadix10;
use std::{collections::HashMap, convert::TryFrom, fs::File, io::Read, ops::Deref, path::Path};

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
            .iter()
            .map(|(s, t)| usize::max(*s, *t))
            .reduce(usize::max)
            .unwrap_or_default()
    }

    pub(crate) fn degrees(&self, node_count: usize, direction: Direction) -> Vec<usize> {
        let mut degrees = vec![0_usize; node_count];

        match direction {
            Direction::Outgoing => self.iter().for_each(|(s, _)| degrees[*s] += 1),
            Direction::Incoming => self.iter().for_each(|(_, t)| degrees[*t] += 1),
            Direction::Undirected => self.iter().for_each(|(s, t)| {
                degrees[*s] += 1;
                degrees[*t] += 1;
            }),
        }

        degrees
    }
}

impl From<&Path> for EdgeList {
    fn from(path: &Path) -> Self {
        let file = File::open(path).unwrap();
        let reader = LineReader::new(file);
        EdgeList::try_from(reader).unwrap()
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
