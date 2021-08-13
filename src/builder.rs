use std::{convert::TryFrom, marker::PhantomData};

use crate::{
    graph::csr::CsrLayout,
    index::Idx,
    input::{EdgeList, InputCapabilities, InputPath},
    Error,
};
use std::path::Path as StdPath;

pub struct Uninitialized {
    csr_layout: CsrLayout,
}

pub struct FromEdges<Node, Edges>
where
    Node: Idx,
    Edges: IntoIterator<Item = (Node, Node)>,
{
    csr_layout: CsrLayout,
    edges: Edges,
    _node: PhantomData<Node>,
}

pub struct FromInput<Node, P, Format>
where
    P: AsRef<StdPath>,
    Node: Idx,
    Format: InputCapabilities<Node>,
    Format::GraphInput: TryFrom<InputPath<P>>,
{
    csr_layout: CsrLayout,
    format: Format,
    _idx: PhantomData<Node>,
    _path: PhantomData<P>,
}

pub struct FromPath<Node, P, Format>
where
    P: AsRef<StdPath>,
    Node: Idx,
    Format: InputCapabilities<Node>,
    Format::GraphInput: TryFrom<InputPath<P>>,
{
    csr_layout: CsrLayout,
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
                csr_layout: CsrLayout::default(),
            },
        }
    }

    pub fn csr_layout(mut self, csr_layout: CsrLayout) -> Self {
        self.state.csr_layout = csr_layout;
        self
    }

    pub fn edges<Node, Edges>(self, edges: Edges) -> GraphBuilder<FromEdges<Node, Edges>>
    where
        Node: Idx,
        Edges: IntoIterator<Item = (Node, Node)>,
    {
        GraphBuilder {
            state: FromEdges {
                csr_layout: self.state.csr_layout,
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
        Format::GraphInput: TryFrom<InputPath<Path>>,
    {
        GraphBuilder {
            state: FromInput {
                csr_layout: self.state.csr_layout,
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
        Graph: From<(EdgeList<Node>, CsrLayout)>,
    {
        Graph::from((
            EdgeList::new(self.state.edges.into_iter().collect()),
            self.state.csr_layout,
        ))
    }
}

impl<Node, Path, Format> GraphBuilder<FromInput<Node, Path, Format>>
where
    Path: AsRef<StdPath>,
    Node: Idx,
    Format: InputCapabilities<Node>,
    Format::GraphInput: TryFrom<InputPath<Path>>,
{
    pub fn path(self, path: Path) -> GraphBuilder<FromPath<Node, Path, Format>> {
        GraphBuilder {
            state: FromPath {
                csr_layout: self.state.csr_layout,
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
    Format::GraphInput: TryFrom<InputPath<Path>>,
    crate::Error: From<<Format::GraphInput as TryFrom<InputPath<Path>>>::Error>,
{
    pub fn build<Graph>(self) -> Result<Graph, Error>
    where
        Graph: TryFrom<(Format::GraphInput, CsrLayout)>,
        crate::Error: From<Graph::Error>,
    {
        let input = Format::GraphInput::try_from(InputPath(self.state.path))?;
        let graph = Graph::try_from((input, self.state.csr_layout))?;

        Ok(graph)
    }
}
