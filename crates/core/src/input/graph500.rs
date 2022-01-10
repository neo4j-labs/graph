use log::info;
use std::{fs::File, marker::PhantomData, path::Path};

use memmap2::Mmap;

use crate::prelude::*;

#[derive(Default)]
pub struct Graph500Input<NI> {
    _phantom: PhantomData<NI>,
}

impl<NI: Idx> InputCapabilities<NI> for Graph500Input<NI> {
    type GraphInput = Graph500<NI>;
}

pub struct Graph500<NI: Idx>(pub EdgeList<NI, ()>);

impl<NI: Idx, P> TryFrom<InputPath<P>> for Graph500<NI>
where
    P: AsRef<Path>,
{
    type Error = Error;

    fn try_from(path: InputPath<P>) -> Result<Self, Self::Error> {
        let start = std::time::Instant::now();

        let file = File::open(path.0.as_ref())?;
        let map = unsafe { Mmap::map(&file)? };
        let file_size = map.len();

        let edge_count = map.len() / std::mem::size_of::<PackedEdge>();

        let map = map.as_ptr();
        assert_eq!(map as usize % std::mem::align_of::<PackedEdge>(), 0);

        let edges = unsafe { std::slice::from_raw_parts(map as *const PackedEdge, edge_count) };

        let edges = edges
            .iter()
            .map(|edge| {
                Ok((
                    NI::new(usize::try_from(edge.source())?),
                    NI::new(usize::try_from(edge.target())?),
                    (),
                ))
            })
            .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;

        let edges = EdgeList::new(edges);

        let elapsed = start.elapsed().as_millis() as f64 / 1000_f64;

        info!(
            "Read {} edges in {:.2}s ({:.2} MB/s)",
            edges.len(),
            elapsed,
            ((file_size as f64) / elapsed) / (1024.0 * 1024.0)
        );

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
