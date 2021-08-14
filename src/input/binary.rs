use std::{
    convert::TryFrom,
    marker::PhantomData,
    path::{Path, PathBuf},
};

use byte_slice_cast::ToByteSlice;

use crate::{index::Idx, Error};

use super::{InputCapabilities, InputPath};

/// Reads a graph that has been written via
/// [`crate::graph_ops::SerializeGraphOp`].
pub struct BinaryInput<Node: Idx + ToByteSlice> {
    _idx: PhantomData<Node>,
}

impl<Node: Idx + ToByteSlice> Default for BinaryInput<Node> {
    fn default() -> Self {
        Self { _idx: PhantomData }
    }
}

impl<Node: Idx + ToByteSlice> InputCapabilities<Node> for BinaryInput<Node> {
    type GraphInput = PathBuf;
}

impl<P> TryFrom<InputPath<P>> for PathBuf
where
    P: AsRef<Path>,
{
    type Error = Error;

    fn try_from(path: InputPath<P>) -> Result<Self, Self::Error> {
        Ok(PathBuf::from(path.0.as_ref()))
    }
}
