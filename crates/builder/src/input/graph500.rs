use log::info;
use std::{
    fs::File,
    marker::PhantomData,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::prelude::*;

#[derive(Default)]
pub struct Graph500Input<NI> {
    _phantom: PhantomData<NI>,
}

impl<NI: Idx> InputCapabilities<NI> for Graph500Input<NI> {
    type GraphInput = Graph500<NI>;
}

pub struct Graph500<NI: Idx>(pub EdgeList<NI, ()>);

impl<NI, P> TryFrom<InputPath<P>> for Graph500<NI>
where
    P: AsRef<Path>,
    NI: Idx,
{
    type Error = Error;

    fn try_from(path: InputPath<P>) -> Result<Self, Self::Error> {
        let file = File::open(path.0.as_ref())?;
        let mmap = unsafe { memmap2::MmapOptions::new().populate().map(&file)? };
        Graph500::try_from(mmap.as_ref())
    }
}

impl<NI> TryFrom<&[u8]> for Graph500<NI>
where
    NI: Idx,
{
    type Error = Error;

    fn try_from(map: &[u8]) -> Result<Self, Self::Error> {
        let start = std::time::Instant::now();

        let file_size = map.len();

        let edge_size = std::mem::size_of::<PackedEdge>();
        let cpu_count = num_cpus::get_physical();
        let chunk_size = usize::max(
            1,
            (map.len() / cpu_count) + (edge_size - 1) & !(edge_size - 1),
        );

        info!("edge_size = {edge_size}, cpu_count = {cpu_count}, chunk_size = {chunk_size}");

        let edges = Arc::new(Mutex::new(Vec::new()));

        rayon::scope(|s| {
            for start in (0..map.len()).step_by(chunk_size) {
                let all_edges = Arc::clone(&edges);

                s.spawn(move |_| {
                    let end = usize::min(start + chunk_size, map.len());
                    let slice = &map[start..end];
                    let local_edge_count = slice.len() / edge_size;

                    let slice = slice.as_ptr();

                    assert_eq!(slice as usize % std::mem::align_of::<PackedEdge>(), 0);

                    let local_edges = unsafe {
                        std::slice::from_raw_parts(slice as *const PackedEdge, local_edge_count)
                    };

                    let mut local_edges = local_edges
                        .iter()
                        .map(|edge| {
                            Ok((
                                NI::new(usize::try_from(edge.source())?),
                                NI::new(usize::try_from(edge.target())?),
                                (),
                            ))
                        })
                        .collect::<Result<Vec<_>, std::num::TryFromIntError>>()
                        .unwrap();

                    let mut all_edges = all_edges.lock().unwrap();
                    all_edges.append(&mut local_edges);
                })
            }
        });

        let edges = Arc::try_unwrap(edges).unwrap().into_inner().unwrap();

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

// impl<NI> TryFrom<&[u8]> for Graph500<NI>
// where
//     NI: Idx,
// {
//     type Error = Error;

//     fn try_from(map: &[u8]) -> Result<Self, Self::Error> {
//         let start = std::time::Instant::now();

//         let file_size = map.len();
//         let edge_count = map.len() / std::mem::size_of::<PackedEdge>();

//         let map = map.as_ptr();
//         assert_eq!(map as usize % std::mem::align_of::<PackedEdge>(), 0);

//         let edges = unsafe { std::slice::from_raw_parts(map as *const PackedEdge, edge_count) };

//         let edges = edges
//             .iter()
//             .map(|edge| {
//                 Ok((
//                     NI::new(usize::try_from(edge.source())?),
//                     NI::new(usize::try_from(edge.target())?),
//                     (),
//                 ))
//             })
//             .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;

//         let edges = EdgeList::new(edges);

//         let elapsed = start.elapsed().as_millis() as f64 / 1000_f64;

//         info!(
//             "Read {} edges in {:.2}s ({:.2} MB/s)",
//             edges.len(),
//             elapsed,
//             ((file_size as f64) / elapsed) / (1024.0 * 1024.0)
//         );

//         Ok(Self(edges))
//     }
// }

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
