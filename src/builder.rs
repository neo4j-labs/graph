use std::{convert::TryFrom, marker::PhantomData};

use crate::{graph::CSROption, index::Idx, input::EdgeList, InputCapabilities};
use std::path::Path as StdPath;

pub struct Uninitialized {
    csr_option: CSROption,
}

pub struct FromEdges<Node, Edges>
where
    Node: Idx,
    Edges: IntoIterator<Item = (Node, Node)>,
{
    csr_option: CSROption,
    edges: Edges,
    _node: PhantomData<Node>,
}

pub struct FromInput<Node, P, Format>
where
    P: AsRef<StdPath>,
    Node: Idx,
    Format: InputCapabilities<Node>,
    Format::GraphInput: TryFrom<crate::input::MyPath<P>>,
{
    csr_option: CSROption,
    format: Format,
    _idx: PhantomData<Node>,
    _path: PhantomData<P>,
}

pub struct FromPath<Node, P, Format>
where
    P: AsRef<StdPath>,
    Node: Idx,
    Format: InputCapabilities<Node>,
    Format::GraphInput: TryFrom<crate::input::MyPath<P>>,
{
    csr_option: CSROption,
    format: Format,
    path: P,
    _idx: PhantomData<Node>,
}

pub struct GraphBuilder<State> {
    state: State,
}

impl Default for GraphBuilder<Uninitialized> {
    fn default() -> Self {
        GraphBuilder::new()
    }
}

impl GraphBuilder<Uninitialized> {
    pub fn new() -> Self {
        Self {
            state: Uninitialized {
                csr_option: CSROption::default(),
            },
        }
    }

    pub fn csr_option(mut self, csr_option: CSROption) -> Self {
        self.state.csr_option = csr_option;
        self
    }

    pub fn edges<Node, Edges>(self, edges: Edges) -> GraphBuilder<FromEdges<Node, Edges>>
    where
        Node: Idx,
        Edges: IntoIterator<Item = (Node, Node)>,
    {
        GraphBuilder {
            state: FromEdges {
                csr_option: self.state.csr_option,
                edges,
                _node: PhantomData,
            },
        }
    }

    pub fn file_format<Format, Path, Node>(
        self,
        format: Format,
    ) -> GraphBuilder<FromInput<Node, Path, Format>>
    where
        Path: AsRef<StdPath>,
        Node: Idx,
        Format: InputCapabilities<Node>,
        Format::GraphInput: TryFrom<crate::input::MyPath<Path>>,
    {
        GraphBuilder {
            state: FromInput {
                csr_option: self.state.csr_option,
                format,
                _idx: PhantomData,
                _path: PhantomData,
            },
        }
    }
}

impl<Node, Edges> GraphBuilder<FromEdges<Node, Edges>>
where
    Node: Idx,
    Edges: IntoIterator<Item = (Node, Node)>,
{
    pub fn build<Graph>(self) -> Graph
    where
        Graph: From<(EdgeList<Node>, CSROption)>,
    {
        Graph::from((
            EdgeList::new(self.state.edges.into_iter().collect()),
            self.state.csr_option,
        ))
    }
}

impl<Node, Path, Format> GraphBuilder<FromInput<Node, Path, Format>>
where
    Path: AsRef<StdPath>,
    Node: Idx,
    Format: InputCapabilities<Node>,
    Format::GraphInput: TryFrom<crate::input::MyPath<Path>>,
{
    pub fn path(self, path: Path) -> GraphBuilder<FromPath<Node, Path, Format>> {
        GraphBuilder {
            state: FromPath {
                csr_option: self.state.csr_option,
                format: self.state.format,
                path,
                _idx: PhantomData,
            },
        }
    }
}

impl<Node, Path, Format> GraphBuilder<FromPath<Node, Path, Format>>
where
    Path: AsRef<StdPath>,
    Node: Idx,
    Format: InputCapabilities<Node>,
    Format::GraphInput: TryFrom<crate::input::MyPath<Path>>,
{
    pub fn build<Graph>(
        self,
    ) -> Result<Graph, <Format::GraphInput as TryFrom<crate::input::MyPath<Path>>>::Error>
    where
        Graph: From<(Format::GraphInput, CSROption)>,
    {
        Ok(Graph::from((
            Format::GraphInput::try_from(crate::input::MyPath(self.state.path))?,
            self.state.csr_option,
        )))
    }
}
