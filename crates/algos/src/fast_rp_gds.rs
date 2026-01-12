use graph_builder::prelude::*;
use ndarray::{Array2, Axis};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::StandardNormal;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};
use std::mem;
use rayon::prelude::ParallelSliceMut;

#[derive(Clone, Debug)] //not Copy bc vector inside
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "clap", derive(clap::Args))]

// TODO:
//  * use original ids for random seed in gds algo
//  * improve parallelization: batch process
//  * write a simpler, paper like version to use as default

pub struct FastRPConfig {
    /// The length of the output vectors.
    #[cfg_attr(feature = "clap", clap(long, default_value_t = FastRPConfig::DEFAULT_OUT_DIM))]
    out_dim: usize,

    /// Coefficients of the polynomial.
    /// Result is (a0 * I  +  a1 * P  +  a2 * P^2  + ...) * L_norm * X_init
    /// where P is the transition matrix used.
    #[cfg_attr(feature = "clap", clap(long, default_values_t = FastRPConfig::DEFAULT_COEFFICIENTS.to_vec()))]
    coefficients: Vec<f32>,

    /// Normalization strength of initial features.
    /// Initial features for node u is scales with ( degree(u) / sum(degrees) ) ^ normalization_strength
    #[cfg_attr(feature = "clap", clap(long, default_value_t = FastRPConfig::DEFAULT_NORMALIZATION_STRENGTH))]
    normalization_strength: f32,

    /// Random seed.
    /// Decides the random features together with the node id.
    /// The algorithm is deterministic for given random features.
    #[cfg_attr(feature = "clap", clap(long, default_value_t = FastRPConfig::DEFAULT_RANDOM_SEED))]
    random_seed: i64,
}

impl FastRPConfig {
    pub const DEFAULT_OUT_DIM: usize = 128;
    pub const DEFAULT_COEFFICIENTS: [f32; 5] = [0., 0., 0., 1., 0.15];
    pub const DEFAULT_NORMALIZATION_STRENGTH: f32 = 0.;
    pub const DEFAULT_RANDOM_SEED: i64 = 0; //fixme: No default. If not given, sample.
    pub fn new(
        out_dim: usize,
        coefficients: Vec<f32>,
        normalization_strength: f32,
        random_seed: i64,
    ) -> Self {
        Self {
            out_dim,
            coefficients,
            normalization_strength,
            random_seed,
        }
    }
}

impl Default for FastRPConfig {
    fn default() -> Self {
        Self {
            out_dim: FastRPConfig::DEFAULT_OUT_DIM,
            coefficients: FastRPConfig::DEFAULT_COEFFICIENTS.to_vec(),
            normalization_strength: FastRPConfig::DEFAULT_NORMALIZATION_STRENGTH,
            random_seed: FastRPConfig::DEFAULT_RANDOM_SEED,
        }
    }
}

struct RandomGenerator {
    u: i64,
    v: i64,
    w: i64,
    seed: i64,
}

impl RandomGenerator {
    const DOUBLE_PRECISION: i32 = 53;
    const DOUBLE_UNIT: f64 = f64::EPSILON * 0.5; // 2^(-53)
    fn new(seed: i64) -> Self {
        let mut x = RandomGenerator {
            u: 0,
            v: 0,
            w: 0,
            seed: 0,
        };
        x.reseed(seed);
        x
    }

    fn reseed(self: &mut Self, seed: i64) {
        self.v = 4101842887655102017i64;
        self.w = 1;
        self.u = seed ^ self.v;
        self.next_long();
        self.v = self.u;
        self.next_long();
        self.w = self.v;
        self.next_long();
    }

    fn next_long(self: &mut Self) -> i64 {
        self.u = self
            .u
            .wrapping_mul(2862933555777941757i64)
            .wrapping_add(7046029254386353087i64);
        self.v ^= ((self.v as u64) >> 17) as i64;
        self.v ^= self.v << 31;
        self.v ^= ((self.v as u64) >> 8) as i64;
        self.w = 4294957665i64
            .wrapping_mul(self.w)
            .wrapping_add(((self.w as u64) >> 32) as i64);
        let mut x = self.u ^ (self.u << 21);
        x ^= ((x as u64) >> 35) as i64;
        x ^= x << 4;
        (x.wrapping_add(self.v)) ^ self.w
    }

    fn next(self: &mut Self, bits: i32) -> i32 {
        ((self.next_long() as u64) >> (64 - bits)) as i32
    }

    pub fn next_double(self: &mut Self) -> f64 {
        (((self.next(Self::DOUBLE_PRECISION - 27) as i64) << 27) + (self.next(27) as i64)) as f64
            * Self::DOUBLE_UNIT
    }
}

fn rnd_original_vec(
    write_vec: &mut [f32],
    dim: usize,
    node_seed: usize,
    random_seed: i64,
    degree: usize,
    normalization_strength: f32,
) {
    const SPARSITY: usize = 3;
    const ENTRY_PROBABILITY: f64 = 1.0 / (2 * SPARSITY) as f64;

    let improved_random_seed = RandomGenerator::new(random_seed).next_long();
    let mut random = RandomGenerator::new((improved_random_seed) ^ (node_seed as i64));
    let scaling = if degree == 0 {
        1.
    } else {
        (degree as f32).powf(normalization_strength)
    };
    let entry_value = scaling * (SPARSITY as f32).sqrt() / (dim as f32).sqrt();

    write_vec.iter_mut().for_each(|x| {
        let random_value = random.next_double();
        *x = if random_value < ENTRY_PROBABILITY {
            entry_value
        } else if random_value < 2. * ENTRY_PROBABILITY {
            -entry_value
        } else {
            0f32
        }
    })
}

fn rnd_gaussian_vec(
    write_vec: &mut [f32],
    node_seed: usize,
    random_seed: i64,
    degree: usize,
    normalization_strength: f32,
) {
    let seed = random_seed ^ (node_seed as i64);
    let mut rng = SmallRng::seed_from_u64(seed as u64);
    let scaling = if degree == 0 {
        1.
    } else {
        (degree as f32).powf(normalization_strength)
    };
    write_vec.iter_mut().for_each(|x| {
        *x = scaling * rng.sample::<f32, StandardNormal>(StandardNormal)
    })
}

pub fn fast_rp<NI, G>(graph: &G, config: FastRPConfig) -> Array2<f32>
where
    NI: Idx,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    let dim = config.out_dim;
    let coefs = config.coefficients;
    let normalization_strength = config.normalization_strength;
    let random_seed = config.random_seed;
    let node_count = graph.node_count().index();

    let mut data: Vec<f32> = vec![f32::default(); node_count * dim];
    data
        .par_chunks_mut(dim)
        .enumerate()
        .for_each(|(u, chunk)| {
            rnd_original_vec(
                chunk,
                dim,
                u,
                random_seed,
                graph.out_degree(NI::new(u)).index(),
                normalization_strength,
            )
        });
    let mut read_matrix = Array2::from_shape_vec((node_count, dim), data).unwrap();
    let mut write_matrix = Array2::zeros((node_count, dim));


    let mut result_matrix: Array2<f32> = if coefs[0] == 0. {
        Array2::zeros((node_count, dim))
    } else {
        Array2::from_shape_fn((node_count, dim), |(u, d)| {
            coefs[0] * read_matrix.get((u, d)).unwrap()
        })
    };

    for &coef in coefs.iter().skip(1) {
        //R, P*R, P^2 * R, P^3 * R ...
        (read_matrix, write_matrix) = propagate_gds(graph, read_matrix, write_matrix);
        mem::swap(&mut read_matrix, &mut write_matrix);

        if coef != 0. {
            for u in 0..node_count {
                let scalar: f32 = if true { //if replicating gds
                    let l2sqr = (0..dim)
                        .map(|d| read_matrix.get((u, d)).unwrap().powi(2))
                        .sum::<f32>();
                    let safe_inv_l2 = if l2sqr == 0. { 1. } else { 1. / l2sqr.sqrt() };
                    coef * safe_inv_l2
                } else {
                    coef
                };

                for (d, value) in result_matrix.row_mut(u).iter_mut().enumerate() {
                    *value += scalar * read_matrix.get((u, d)).unwrap();
                }
            }
        }
        write_matrix
            .axis_iter_mut(Axis(0))
            .into_par_iter()
            .for_each(|mut write_vec_u| {
                for w_ud in write_vec_u.iter_mut() {
                    *w_ud = 0f32;
                }
            })
    }
    result_matrix
}

fn propagate_gds<NI, G>(
    graph: &G,
    mut read_matrix: Array2<f32>,
    mut write_matrix: Array2<f32>,
) -> (Array2<f32>, Array2<f32>)
where
    NI: Idx,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    write_matrix
        .axis_iter_mut(Axis(0))
        .into_par_iter()
        .enumerate()
        .for_each(|(u, mut write_vec_u)| {
            for v in graph.out_neighbors(NI::new(u)) {
                let inv_deg = 1f32 / graph.out_degree(NI::new(u)).index() as f32;
                let read_vec_v = read_matrix.row(v.index());
                for (w_ud, r_ud) in write_vec_u.iter_mut().zip(read_vec_v.iter()) {
                    *w_ud += inv_deg * r_ud;
                }
            }
        });

    (read_matrix, write_matrix)
}

fn propagate<NI, G>(
    graph: &G,
    mut read_matrix: Array2<f32>,
    mut write_matrix: Array2<f32>,
) -> (Array2<f32>, Array2<f32>)
where
    NI: Idx,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    read_matrix
        .axis_iter_mut(Axis(0))
        .into_par_iter()
        .enumerate()
        .for_each(|(u, mut read_vec_u)| {
            let inv_deg = 1f32 / graph.out_degree(NI::new(u)).index() as f32;
            for r_ud in read_vec_u.iter_mut() {
                *r_ud *= inv_deg;
            }
        });

    write_matrix
        .axis_iter_mut(Axis(0))
        .into_par_iter()
        .enumerate()
        .for_each(|(u, mut write_vec_u)| {
            for v in graph.out_neighbors(NI::new(u)) {
                let read_vec_v = read_matrix.row(v.index());
                for (w_ud, r_ud) in write_vec_u.iter_mut().zip(read_vec_v.iter()) {
                    *w_ud += r_ud;
                }
            }
        });
    (read_matrix, write_matrix)
}

#[cfg(test)]
mod tests {
    use crate::fast_rp_gds::{fast_rp, FastRPConfig};
    use crate::prelude::*;
    const TOLERANCE: f32 = 1e-6;

    #[test]
    fn test_fast_rp_0() {
        let gdl = "(a0),(a1),\n(a0)-[:R]->(a1),(a1)-[:R]->(a0)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        for u in 0..graph.node_count().index() {
            for v in graph.out_neighbors(u) {
                println!("    ({u}) --> ({v})")
            }
        }

        let nd_vectors = fast_rp(
            &graph,
            FastRPConfig::new(8, vec![0., 0., 0., 1., 0.15], 0., 0),
        );
        let vectors: Vec<Vec<f32>> = nd_vectors
            .rows()
            .into_iter()
            .map(|row| row.to_vec())
            .collect();

        for (u, vector) in vectors.iter().enumerate() {
            println!("{u}: {vector:?}");
        }

        let expected_vecs: Vec<Vec<f32>> = vec![
            vec![
                0.0,
                0.08660254,
                -0.49074772,
                0.0,
                0.57735026,
                0.0,
                0.6639528,
                0.0,
            ],
            vec![
                0.0, 0.57735026, 0.49074772, 0.0, 0.08660254, 0.0, 0.6639528, 0.0,
            ],
        ];

        for i in 0..2 {
            for (actual, expected) in vectors[i].iter().zip(expected_vecs[i].clone()) {
                assert!((actual - expected).abs() < TOLERANCE)
            }
        }
    }

    #[test]
    fn test_fast_rp() {
        let gdl = "(a0),(a1),(a2),(a3),\
        (a0)-->(a1),(a1)-->(a0),(a0)-->(a2),(a3)-->(a2)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        for u in 0..graph.node_count().index() {
            for v in graph.out_neighbors(u) {
                println!("    ({u}) --> ({v})")
            }
        }

        let nd_vectors = fast_rp(&graph, FastRPConfig::default());
        let vectors: Vec<Vec<f32>> = nd_vectors
            .rows()
            .into_iter()
            .map(|row| row.to_vec())
            .collect();

        for (u, vector) in vectors.iter().enumerate() {
            println!("{u}: {vector:?}");
        }

        let expected_vecs: Vec<Vec<f32>> = vec![
            vec![
                0.0,
                0.12577915,
                -0.18040705,
                0.10206207,
                0.10206207,
                0.0,
                0.023717085,
                0.10206207,
            ],
            vec![
                0.0,
                0.1734232,
                0.12749526,
                0.015309311,
                0.015309311,
                0.0,
                0.15811388,
                0.015309311,
            ],
            vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ];

        for i in 0..4 {
            for (actual, expected) in vectors[i][..8].iter().zip(expected_vecs[i].clone()) {
                if (actual - expected).abs() > TOLERANCE {
                    panic!("Actual: {actual}, Expected: {expected} at vector {i}")
                }
            }
        }
    }
}
