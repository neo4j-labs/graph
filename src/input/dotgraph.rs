use std::{
    convert::TryFrom, fmt::Write, fs::File, hash::Hash, intrinsics::transmute, io::Read,
    marker::PhantomData, path::Path, sync::atomic::Ordering::Acquire,
};

use fxhash::FxHashMap;
use linereader::LineReader;
use rayon::prelude::*;

use crate::{
    graph::csr::{sort_targets, Csr},
    index::{AtomicIdx, Idx},
    Error, SharedMut,
};

use super::{EdgeList, InputCapabilities, InputPath};

/// DotGraph (the name is based on the file ending `.graph`) is a textual
/// description of a node labeled graph primarily used as input for subgraph
/// isomorphism libraries. It has been introduced
/// [here](https://github.com/RapidsAtHKUST/SubgraphMatching#input) and is also
/// supported by the
/// [subgraph-matching](https://crates.io/crates/subgraph-matching) crate.
///
/// A graph starts with 't N M' where N is the number of nodes and M is the
/// number of edges. A node and an edge are formatted as 'v nodeId labelId
/// degree' and 'e nodeId nodeId' respectively. Note that the format requires
/// that the node id starts at 0 and the range is `0..N`.
///
/// # Example
///
/// The following graph contains 5 nodes and 6 relationships. The first line
/// contains that meta information. The following 5 lines contain one node
/// description per line, e.g., `v 0 0 2` translates to node `0` with label `0`
/// and a degree of `2`. Following the nodes, the remaining lines describe
/// edges, e.g., `e 0 1` represents an edge connecting nodes `0` and `1`.
///
/// ```ignore
/// > cat my_graph.graph
/// t 5 6
/// v 0 0 2
/// v 1 1 3
/// v 2 2 3
/// v 3 1 2
/// v 4 2 2
/// e 0 1
/// e 0 2
/// e 1 2
/// e 1 3
/// e 2 4
/// e 3 4
/// ```
pub struct DotGraphInput<Node, Label>
where
    Node: Idx,
    Label: Idx,
{
    _phantom: PhantomData<(Node, Label)>,
}

impl<Node, Label> Default for DotGraphInput<Node, Label>
where
    Node: Idx,
    Label: Idx,
{
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<Node: Idx, Label: Idx> InputCapabilities<Node> for DotGraphInput<Node, Label> {
    type GraphInput = DotGraph<Node, Label>;
}

pub struct DotGraph<Node, Label>
where
    Node: Idx,
    Label: Idx,
{
    pub(crate) labels: Vec<Label>,
    pub(crate) edge_list: EdgeList<Node>,
    pub(crate) max_degree: Node,
    pub(crate) max_label: Label,
    pub(crate) label_frequencies: FxHashMap<Label, usize>,
}

impl<Node, Label> DotGraph<Node, Label>
where
    Node: Idx,
    Label: Idx + Hash,
{
    fn node_count(&self) -> Node {
        Node::new(self.labels.len())
    }

    pub(crate) fn label_count(&self) -> Label {
        Label::new(self.max_label.index() + 1)
    }

    pub(crate) fn max_label_frequency(&self) -> usize {
        self.label_frequencies
            .values()
            .max()
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn label_index(&self) -> Csr<Label, Node> {
        let node_count = self.node_count();
        let label_count = self.label_count();

        // Prefix sum: We insert the offset entries one index to the right and
        // increment the offset of the next label during insert. That way we'll
        // end up with the correct offsets after inserting into `nodes` in the
        // next loop.
        let mut offsets = Vec::with_capacity(label_count.index() + 1);
        offsets.push(Label::zero());

        let mut total = Label::zero();
        for label in Label::zero()..=self.max_label {
            offsets.push(total);
            total += Label::new(*self.label_frequencies.get(&label).unwrap_or(&0));
        }

        // SAFETY: Label and Label::Atomic have the same memory layout
        let offsets = unsafe { transmute::<_, Vec<Label::Atomic>>(offsets) };

        let mut nodes = Vec::<Node>::with_capacity(node_count.index());
        let nodes_ptr = SharedMut::new(nodes.as_mut_ptr());

        self.labels
            .par_iter()
            .enumerate()
            .for_each(|(node, &label)| {
                let next_label = label + Label::new(1);
                let offset = offsets[next_label.index()].fetch_add(Label::new(1), Acquire);
                // SAFETY: There is exactly one thread that writes at `offset.index()`.
                unsafe {
                    nodes_ptr.add(offset.index()).write(Node::new(node));
                }
            });

        // SAFETY: The `labels` vec has `node_count` length and we performed an
        // insert operation for each index (node). Each inserts happens at a
        // unique index which is computed from the `offset` array.
        unsafe {
            nodes.set_len(node_count.index());
        }

        // SAFETY: Label and Label::Atomic have the same memory layout
        let offsets = unsafe { transmute::<_, Vec<Label>>(offsets) };

        sort_targets(&offsets, &mut nodes);

        let offsets = offsets.into_boxed_slice();
        let nodes = nodes.into_boxed_slice();

        Csr::new(offsets, nodes)
    }
}

impl<Node, Label, P> TryFrom<InputPath<P>> for DotGraph<Node, Label>
where
    P: AsRef<Path>,
    Node: Idx,
    Label: Idx + Hash,
{
    type Error = Error;

    fn try_from(path: InputPath<P>) -> Result<Self, Self::Error> {
        let file = File::open(path.0.as_ref())?;
        let reader = LineReader::new(file);
        let dot_graph = DotGraph::try_from(reader)?;
        Ok(dot_graph)
    }
}

impl<Node, Label, R> TryFrom<LineReader<R>> for DotGraph<Node, Label>
where
    Node: Idx,
    Label: Idx + Hash,
    R: Read,
{
    type Error = Error;

    /// Converts the given .graph input into a [`DotGraph`].
    fn try_from(mut lines: LineReader<R>) -> Result<Self, Self::Error> {
        let mut header = lines.next_line().expect("missing header line")?;

        // skip "t" char and white space
        header = &header[2..];
        let (node_count, used) = Node::parse(header);
        header = &header[used + 1..];
        let (edge_count, _) = Node::parse(header);

        let mut labels = Vec::<Label>::with_capacity(node_count.index());
        let mut edges = Vec::with_capacity(edge_count.index());

        let mut max_degree = Node::zero();
        let mut max_label = Label::zero();
        let mut label_frequency = FxHashMap::<Label, usize>::default();

        let mut batch = lines.next_batch().expect("missing data")?;

        for _ in 0..node_count.index() {
            if batch.is_empty() {
                batch = lines.next_batch().expect("missing data")?;
            }

            // skip "v" char and white space
            batch = &batch[2..];
            // skip node id since input is always sorted by node id
            let (_, used) = Node::parse(batch);
            batch = &batch[used + 1..];
            let (label, used) = Label::parse(batch);
            batch = &batch[used + 1..];
            let (degree, used) = Node::parse(batch);
            batch = &batch[used + 1..];

            labels.push(label);

            if degree > max_degree {
                max_degree = degree;
            }

            let frequency = label_frequency.entry(label).or_insert_with(|| {
                if label > max_label {
                    max_label = label;
                }
                0
            });
            *frequency += 1;
        }

        for _ in 0..edge_count.index() {
            if batch.is_empty() {
                batch = lines.next_batch().expect("missing data")?;
            }
            // skip "e" char and white space
            batch = &batch[2..];
            let (source, used) = Node::parse(batch);
            batch = &batch[used + 1..];
            let (target, used) = Node::parse(batch);
            batch = &batch[used + 1..];

            edges.push((source, target));
        }

        let edges = EdgeList::new(edges);

        Ok(Self {
            labels,
            edge_list: edges,
            max_degree,
            max_label,
            label_frequencies: label_frequency,
        })
    }
}

impl<Node, Label> From<&gdl::Graph> for DotGraph<Node, Label>
where
    Node: Idx,
    Label: Idx + Hash,
{
    /// Converts the given GDL graph into a .graph input string.
    ///
    /// Node labels need to be numeric, however GDL does not support numeric
    /// labels. In order to circumvent this, node labels need to be prefixed
    /// with a single character, e.g. `(n:L0)` to declare label `0`.
    fn from(gdl_graph: &gdl::Graph) -> Self {
        fn degree(gdl_graph: &gdl::Graph, node: &gdl::graph::Node) -> usize {
            let mut degree = 0;

            for rel in gdl_graph.relationships() {
                if rel.source() == node.variable() {
                    degree += 1;
                }
                if rel.target() == node.variable() {
                    degree += 1;
                }
            }
            degree
        }

        let header = format!(
            "t {} {}",
            gdl_graph.node_count(),
            gdl_graph.relationship_count()
        );

        let mut nodes_string = String::from("");

        let mut sorted_nodes = gdl_graph.nodes().collect::<Vec<_>>();
        sorted_nodes.sort_by_key(|node| node.id());

        for node in sorted_nodes {
            let id = node.id();
            let label = node.labels().next().expect("Single label expected");
            let degree = degree(gdl_graph, node);
            let _ = writeln!(nodes_string, "v {} {} {}", id, &label[1..], degree);
        }

        let mut rels_string = String::from("");

        let mut sorted_rels = gdl_graph.relationships().collect::<Vec<_>>();
        sorted_rels.sort_by_key(|rel| (rel.source(), rel.target()));

        for rel in sorted_rels {
            let source_id = gdl_graph
                .get_node(rel.source())
                .expect("Source expected")
                .id();
            let target_id = gdl_graph
                .get_node(rel.target())
                .expect("Target expected")
                .id();
            let _ = writeln!(rels_string, "e {} {}", source_id, target_id);
        }

        let input = format!("{}\n{}{}", header, nodes_string, rels_string);
        let reader = LineReader::new(input.as_bytes());

        DotGraph::<Node, Label>::try_from(reader).expect("GDL to .graph conversion failed")
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::input::InputPath;

    use super::*;

    const TEST_GRAPH: [&str; 3] = [env!("CARGO_MANIFEST_DIR"), "resources", "test.graph"];

    #[test]
    fn dotgraph_from_file() {
        let path = TEST_GRAPH.iter().collect::<PathBuf>();
        let graph = DotGraph::<usize, usize>::try_from(InputPath(path.as_path())).unwrap();

        assert_eq!(graph.labels.len(), 5);
        assert_eq!(graph.edge_list.len(), 6);
        assert_eq!(graph.max_label, 2);
        assert_eq!(graph.max_degree, 3);
    }

    #[test]
    fn label_test() {
        let path = TEST_GRAPH.iter().collect::<PathBuf>();
        let graph = DotGraph::<usize, usize>::try_from(InputPath(path.as_path())).unwrap();

        assert_eq!(graph.max_label_frequency(), 2);
    }
}
