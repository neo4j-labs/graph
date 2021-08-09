use std::{collections::HashMap, convert::TryFrom, marker::PhantomData, path::Path};

use crate::{index::Idx, Error, InputCapabilities};

use super::MyPath;

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
