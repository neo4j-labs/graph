use std::{fs::File, path::Path};

use memmap2::Mmap;

use crate::prelude::*;

#[derive(Default)]
pub struct Graph500Input;

impl InputCapabilities<u64> for Graph500Input {
    type GraphInput = Graph500;
}

pub struct Graph500(pub EdgeList<u64, ()>);

impl<P> TryFrom<InputPath<P>> for Graph500
where
    P: AsRef<Path>,
{
    type Error = Error;

    fn try_from(path: InputPath<P>) -> Result<Self, Self::Error> {
        let file = File::open(path.0.as_ref())?;
        let map = unsafe { Mmap::map(&file)? };
        let edge_count = map.len() / std::mem::size_of::<PackedEdge>();

        let map = map.as_ptr();
        assert_eq!(map as usize % std::mem::align_of::<PackedEdge>(), 0);
        let edges = unsafe { std::slice::from_raw_parts(map as *const PackedEdge, edge_count) };

        let edges = edges
            .iter()
            .map(|edge| (edge.source(), edge.target(), ()))
            .collect::<Vec<_>>();

        let edges = EdgeList::new(edges);

        Ok(Self(edges))
    }
}

// see https://github.com/graph500/graph500/blob/f89d643ce4aaae9a823d310c6ab2dd10e3d2982c/generator/graph_generator.h#L29-L33
#[derive(Default, Copy, Clone, Debug)]
#[repr(C)]
struct PackedEdge {
    v0_low: u32,
    v1_low: u32,
    high: u32,
}

impl PackedEdge {
    fn source(&self) -> u64 {
        self.v0_low as u64 | (self.high as u64 & 0xFFFF) << 32
    }

    fn target(&self) -> u64 {
        self.v1_low as u64 | (self.high as u64 >> 16) << 32
    }
}
