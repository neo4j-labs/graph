use std::{
    collections::HashMap, convert::TryFrom, fs::File, hash::Hash, io::Read, marker::PhantomData,
    path::Path,
};

use linereader::LineReader;

use crate::{index::Idx, Error};

use super::{EdgeList, InputCapabilities, InputPath};

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

pub struct DotGraph<Node: Idx, Label: Idx> {
    node_count: Node,
    edge_count: Node,
    labels: Vec<Label>,
    edges: EdgeList<Node>,
    max_degree: Node,
    max_label: Label,
    label_frequency: HashMap<Label, usize>,
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
        let mut label_frequency = HashMap::<Label, usize>::new();

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
            node_count,
            edge_count,
            labels,
            edges,
            max_degree,
            max_label,
            label_frequency,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::input::InputPath;

    use super::*;

    #[test]
    fn dotgraph_from_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.graph"]
            .iter()
            .collect::<PathBuf>();

        let DotGraph {
            node_count,
            edge_count,
            labels,
            edges,
            max_degree,
            max_label,
            label_frequency: _,
        } = DotGraph::<usize, usize>::try_from(InputPath(path.as_path())).unwrap();

        assert_eq!(node_count, 5);
        assert_eq!(edge_count, 6);
        assert_eq!(labels.len(), 5);
        assert_eq!(edges.len(), 6);
        assert_eq!(max_label, 2);
        assert_eq!(max_degree, 3);
    }
}
