use atoi::FromRadix10;
use num_format::{Locale, ToFormattedString};
use std::{
    collections::HashMap, convert::TryFrom, fs::File, io::Read, ops::Deref, path::Path,
    time::Instant,
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

    fn try_from(mut lines: LineReader<R>) -> Result<Self, Self::Error> {
        let mut edges = Vec::new();
        let start = Instant::now();
        let mut batch = lines.next_batch().expect("missing data")?;

        loop {
            if batch.is_empty() {
                if let Some(next_batch) = lines.next_batch() {
                    let edge_count = edges.len();
                    let duration = usize::max(start.elapsed().as_secs() as usize, 1);
                    let tp = edges.len() / duration;
                    println!(
                        "read {} edges ({} edges / second)",
                        edge_count.to_formatted_string(&Locale::en),
                        tp.to_formatted_string(&Locale::en)
                    );
                    batch = next_batch.expect("missing data");
                } else {
                    break;
                }
            }

            let (source, used) = usize::from_radix_10(batch);
            batch = &batch[used + 1..];
            let (target, used) = usize::from_radix_10(batch);
            batch = &batch[used + 1..];

            edges.push((source, target));
        }

        println!(
            "read {} edges in {} seconds",
            edges.len(),
            start.elapsed().as_secs().to_formatted_string(&Locale::en)
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
