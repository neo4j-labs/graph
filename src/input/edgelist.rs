use log::info;
use std::{
    convert::TryFrom,
    fs::File,
    marker::PhantomData,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::index::{AtomicIdx, Idx};

use rayon::prelude::*;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::Ordering::AcqRel,
};

use crate::{input::Direction, Error};

use super::{InputCapabilities, InputPath, MyCypherValue, ParseValue};

/// Reads a graph from a file that contains an edge per line.
///
/// An edge is represented by a source node id and a target node id. The two
/// node ids must be separated by a 1-byte character (e.g. whitespace or tab).
///
/// The node count of the resulting graph is the highest node id within the file
/// plus one. The edge count will be twice the number of lines in the file.
///
/// # Example
///
/// ```ignore
/// > cat my_graph.edgelist
/// 0 1
/// 0 2
/// 1 3
/// 2 0
/// ```
pub struct EdgeListInput<NI: Idx, EV = ()> {
    _idx: PhantomData<(NI, EV)>,
}

impl<NI: Idx, EV> Default for EdgeListInput<NI, EV> {
    fn default() -> Self {
        Self { _idx: PhantomData }
    }
}

impl<NI: Idx, EV> InputCapabilities<NI> for EdgeListInput<NI, EV> {
    type GraphInput = EdgeList<NI, EV>;
}

#[derive(Debug)]
pub struct EdgeList<NI: Idx, EV>(Box<[(NI, NI, EV)]>);

impl<NI: Idx, EV> AsRef<[(NI, NI, EV)]> for EdgeList<NI, EV> {
    fn as_ref(&self) -> &[(NI, NI, EV)] {
        &self.0
    }
}

impl<NI: Idx, EV> Deref for EdgeList<NI, EV> {
    type Target = [(NI, NI, EV)];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<NI: Idx, EV> DerefMut for EdgeList<NI, EV> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<NI: Idx, EV: Sync> EdgeList<NI, EV> {
    pub fn new(edges: Vec<(NI, NI, EV)>) -> Self {
        Self(edges.into_boxed_slice())
    }

    pub fn max_node_id(&self) -> NI {
        self.par_iter()
            .map(|(s, t, _)| NI::max(*s, *t))
            .reduce(NI::zero, NI::max)
    }

    pub fn degrees(&self, node_count: NI, direction: Direction) -> Vec<NI::Atomic> {
        let mut degrees = Vec::with_capacity(node_count.index());
        degrees.resize_with(node_count.index(), NI::Atomic::zero);

        if matches!(direction, Direction::Outgoing | Direction::Undirected) {
            self.par_iter().for_each(|(s, _, _)| {
                degrees[s.index()].get_and_increment(AcqRel);
            });
        }

        if matches!(direction, Direction::Incoming | Direction::Undirected) {
            self.par_iter().for_each(|(_, t, _)| {
                degrees[t.index()].get_and_increment(AcqRel);
            });
        }

        degrees
    }
}

impl<'gdl, NI, EV> From<&'gdl gdl::Graph> for EdgeList<NI, EV>
where
    NI: Idx,
    EV: From<MyCypherValue<'gdl>> + Default + Send + Sync,
{
    fn from(gdl_graph: &'gdl gdl::Graph) -> Self {
        let edges = gdl_graph
            .relationships()
            .into_iter()
            .map(|r| {
                let source = gdl_graph.get_node(r.source()).unwrap().id();
                let target = gdl_graph.get_node(r.target()).unwrap().id();

                let value = if let Some(k) = r.property_keys().next() {
                    EV::from(MyCypherValue(r.property_value(k).unwrap()))
                } else {
                    EV::default()
                };

                (NI::new(source), NI::new(target), value)
            })
            .collect::<Vec<_>>();

        EdgeList::new(edges)
    }
}

impl<NI, P, EV> TryFrom<InputPath<P>> for EdgeList<NI, EV>
where
    P: AsRef<Path>,
    NI: Idx,
    EV: ParseValue + std::fmt::Debug + Send + Sync,
{
    type Error = Error;

    fn try_from(path: InputPath<P>) -> Result<Self, Self::Error> {
        let file = File::open(path.0.as_ref())?;
        let mmap = unsafe { memmap2::MmapOptions::new().populate().map(&file)? };
        EdgeList::try_from(mmap.as_ref())
    }
}

impl<NI, EV> TryFrom<&[u8]> for EdgeList<NI, EV>
where
    NI: Idx,
    EV: ParseValue + std::fmt::Debug + Send + Sync,
{
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
                        let (source, source_bytes) = NI::parse(chunk);
                        chunk = &chunk[source_bytes + 1..];

                        let (target, target_bytes) = NI::parse(chunk);
                        chunk = &chunk[target_bytes..];

                        let value = match chunk.strip_prefix(b" ") {
                            Some(value_chunk) => {
                                let (value, value_bytes) = EV::parse(value_chunk);
                                chunk = &value_chunk[value_bytes + 1..];
                                value
                            }
                            None => {
                                chunk = &chunk[1..];
                                // if the input does not have a value, the default for EV is used
                                EV::parse(&[]).0
                            }
                        };

                        edges.push((source, target, value));
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

        let expected: Vec<(usize, usize, ())> = vec![
            (0, 1, ()),
            (0, 2, ()),
            (1, 2, ()),
            (1, 3, ()),
            (2, 4, ()),
            (3, 4, ()),
        ];

        let edge_list = EdgeList::<usize, ()>::try_from(InputPath(path.as_path())).unwrap();

        assert_eq!(4, edge_list.max_node_id());

        let edge_list = edge_list.0.into_vec();

        assert_eq!(expected, edge_list)
    }

    #[test]
    fn edge_list_with_values_from_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.wel"]
            .iter()
            .collect::<PathBuf>();

        let expected: Vec<(usize, usize, f32)> = vec![
            (0, 1, 0.1),
            (0, 2, 0.2),
            (1, 2, 0.3),
            (1, 3, 0.4),
            (2, 4, 0.5),
            (3, 4, 0.6),
        ];

        let edge_list = EdgeList::<usize, f32>::try_from(InputPath(path.as_path())).unwrap();

        assert_eq!(4, edge_list.max_node_id());

        let edge_list = edge_list.0.into_vec();

        assert_eq!(expected, edge_list)
    }
}
