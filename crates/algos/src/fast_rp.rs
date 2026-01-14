//! Fast Random Projections (FastRP) algorithm.
//!
//! FastRP[1] computes node embeddings as random projections of powers of the graph
//! transition matrix P (n x n), i.e. the column-normalized adjacency matrix. The
//! per-node embeddings are defined as  (a0 * P^0 + a1 * P^1 + a2 * P^2 + ...) * X_0,
//! where X_0 (n x d, d << n) are random vectors X_init scaled with
//! (degree(u) / (2 * edge_count)) ^ `normalization_strength`
//!
//! The implementation uses sparse x dense matrix multiplication, efficiently
//! computing (P^i * X_0) as P * (P^(i-1) * X_0). The result is then computed as a
//! weighted sum of the different (P^i * X_0).
//!
//! Compute time should scale as O(`out_dim` * edge_count * `coefficients.len()`), and
//! memory usage as O(out_dim * node_count + edge_count). Where `out_dim` is the desired
//! length of the embeddings.
//!
//! This algorithm offers two implementations. One default, designed to be similar to
//! the paper [1] and one alternative, which is implemented to be consistent with the
//! Neo4j Graph Data Science library see [2].
//!
//! For the default implementation, the embeddings are initialized as random Gaussian
//! vectors (mean=0, std=1). For the GDS-consistent implementation, the embeddings are
//! initialized as sparse vectors with values +-`SPARSITY` or 0, see [2], also
//! suggested in [1].
//!
//! The random initialization is determined by the `common_random_seed` and the
//! `node_random_seed`. This assures consistency between runs even when the node ids
//! are changed. If `node_random_seed` is set to `None`, it defaults to using node ids.
//! If `common_random_seed` is set to `None` it is instead randomly generated.
//!
//! [1] Haochen Chen, Syed Fahad Sultan, Yingtao Tian, Muhao Chen, and Steven Skiena.
//! 2019. Fast and Accurate Network Embeddings via Very Sparse Random Projection.
//! In Proceedings of the 28th ACM International Conference on Information and Knowledge Management (CIKM '19).
//! Association for Computing Machinery, New York, NY, USA, 399–408. https://doi.org/10.1145/3357384.3357879
//! [2] https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/fastrp/

use graph_builder::prelude::*;
use ndarray::{Array2, ArrayView1, ArrayViewMut1, Axis, Zip};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::StandardNormal;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::mem;

#[derive(Clone, Debug)] //not Copy bc vector inside
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "clap", derive(clap::Args))]

pub struct FastRPConfig {
    /// The length of the output vectors. A larger graph, usually requires a larger embedding space,
    /// but this does not scale linearly (curse of dimensionality). Closely related to
    /// compute time and memory usage, see documentation above.
    #[cfg_attr(feature = "clap", clap(long, default_value_t = FastRPConfig::DEFAULT_OUT_DIM))]
    out_dim: usize,

    /// Coefficients of the polynomial, input as [a0, a1, ...].
    /// Default is [0., 0., 0., 1., 0.15] -> x^3 + 0.15 x^4
    #[cfg_attr(feature = "clap", clap(long, default_values_t = FastRPConfig::DEFAULT_COEFFICIENTS.to_vec()
    ))]
    coefficients: Vec<f32>,

    /// Normalization strength of initial features.
    /// Scales the random vectors with L = ( degree(u) / (2*edge_count) ) ^ normalization_strength.
    #[cfg_attr(feature = "clap", clap(long, default_value_t = FastRPConfig::DEFAULT_NORMALIZATION_STRENGTH
    ))]
    normalization_strength: f32,

    /// Common random seed
    /// Determines the random initialization together with node seeds.
    /// If not given, it is randomly initialized.
    #[cfg_attr(feature = "clap", clap(long))]
    common_random_seed: Option<i64>,

    /// Node random seeds
    /// The node specific feature that determines the initialization of the random vectors.
    /// The common random seed must also be set for consistency between runs.
    /// If not given, the node id is used.
    #[cfg_attr(feature = "clap", clap(long))]
    node_random_seeds: Option<Vec<i64>>,

    /// Use legacy gds version
    #[cfg_attr(
        feature = "clap",
        clap(long, default_value_t = FastRPConfig::DEFAULT_GDS_CONSISTENT)
    )]
    gds_consistent: bool,
}

impl FastRPConfig {
    pub const DEFAULT_OUT_DIM: usize = 128;
    pub const DEFAULT_COEFFICIENTS: [f32; 5] = [0., 0., 0., 1., 0.15];
    pub const DEFAULT_NORMALIZATION_STRENGTH: f32 = 0.;
    pub const DEFAULT_COMMON_RANDOM_SEED: Option<i64> = None;
    pub const DEFAULT_NODE_RANDOM_SEEDS: Option<Vec<i64>> = None;
    pub const DEFAULT_GDS_CONSISTENT: bool = false;
    pub fn new(
        out_dim: usize,
        coefficients: Vec<f32>,
        normalization_strength: f32,
        common_random_seed: Option<i64>,
        node_random_seeds: Option<Vec<i64>>,
        gds_consistent: bool,
    ) -> Self {
        Self {
            out_dim,
            coefficients,
            normalization_strength,
            common_random_seed,
            node_random_seeds,
            gds_consistent,
        }
    }
}

impl Default for FastRPConfig {
    fn default() -> Self {
        Self {
            out_dim: FastRPConfig::DEFAULT_OUT_DIM,
            coefficients: FastRPConfig::DEFAULT_COEFFICIENTS.to_vec(),
            normalization_strength: FastRPConfig::DEFAULT_NORMALIZATION_STRENGTH,
            common_random_seed: FastRPConfig::DEFAULT_COMMON_RANDOM_SEED,
            node_random_seeds: FastRPConfig::DEFAULT_NODE_RANDOM_SEEDS,
            gds_consistent: FastRPConfig::DEFAULT_GDS_CONSISTENT,
        }
    }
}

pub fn fast_rp<NI, G>(graph: &G, config: FastRPConfig) -> Array2<f32>
where
    NI: Idx,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    let FastRPConfig {
        out_dim: dim,
        coefficients,
        normalization_strength,
        common_random_seed: maybe_common_random_seed,
        node_random_seeds: maybe_node_random_seeds,
        gds_consistent,
    } = config;

    let node_count = graph.node_count().index();
    let common_random_seed = maybe_common_random_seed.unwrap_or_else(|| rand::rng().random());
    if let Some(node_seeds) = &maybe_node_random_seeds {
        assert_eq!(node_seeds.len(), node_count);
        //fixme: warn if common_random_seed not set but node specific is
    }

    let (rnd_vec_fn, propagate_fn, scale_fn): (
        fn(&mut [f32], i64, i64, usize, f32, f32),
        fn(&G, Array2<f32>, Array2<f32>) -> (Array2<f32>, Array2<f32>),
        fn(ArrayViewMut1<f32>, ArrayView1<f32>, f32),
    ) = match gds_consistent {
        true => (rnd_vec_gds, propagate_gds, scale_vec_gds),
        false => (rnd_gaussian_vec, propagate, scale_vec),
    };

    let inv_degree_sum = 1. / ( 2 * graph.edge_count().index()) as f32;
    let mut read_matrix = Array2::zeros((node_count, dim));
    read_matrix
        .axis_iter_mut(Axis(0))
        .into_par_iter()
        .enumerate()
        .for_each(|(u, mut row)| {
            let node_seed = match &maybe_node_random_seeds {
                None => u as i64,
                Some(node_seeds) => node_seeds[u],
            };
            rnd_vec_fn(
                row.as_slice_mut().unwrap(),
                node_seed,
                common_random_seed,
                graph.out_degree(NI::new(u)).index(),
                inv_degree_sum,
                normalization_strength,
            )
        });
    let mut write_matrix = Array2::zeros((node_count, dim));
    let mut result_matrix: Array2<f32> = Array2::zeros((node_count, dim));

    if let Some(coef) = coefficients.first() {
        if *coef != 0f32 {
            Zip::from(&mut result_matrix)
                .and(&read_matrix)
                .par_for_each(|result_val, &read_val| {
                    *result_val = coef * read_val;
                });
        }
    };

    for &coef in coefficients.iter().skip(1) {
        //R, P*R, P^2 * R, P^3 * R ...
        (read_matrix, write_matrix) = propagate_fn(graph, read_matrix, write_matrix);
        mem::swap(&mut read_matrix, &mut write_matrix);

        if coef != 0. {
            result_matrix
                .axis_iter_mut(Axis(0))
                .into_par_iter()
                .enumerate()
                .for_each(|(u, mut result_vec_u)| {
                    scale_fn(result_vec_u, read_matrix.row(u), coef);
                });
        }
        write_matrix
            .axis_iter_mut(Axis(0))
            .into_par_iter()
            .for_each(|mut write_vec_u| {
                for w_ud in write_vec_u.iter_mut() {
                    *w_ud = 0f32;
                }
            });
    }
    result_matrix
}

fn rnd_gaussian_vec(
    write_vec: &mut [f32],
    node_seed: i64,
    random_seed: i64,
    degree: usize,
    inv_degree_sum: f32,
    normalization_strength: f32,
) {
    let seed = random_seed ^ node_seed;
    let mut rng = SmallRng::seed_from_u64(seed as u64);
    let scaling = (degree.max(1) as f32 * inv_degree_sum).powf(normalization_strength);
    write_vec
        .iter_mut()
        .for_each(|x| *x = scaling * rng.sample::<f32, StandardNormal>(StandardNormal))
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
            let deg = graph.out_degree(NI::new(u)).index();
            let inv_deg = if deg > 0 { 1f32 / deg as f32 } else { 1f32 };
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

fn scale_vec(mut result_vec_u: ArrayViewMut1<f32>, read_vec_u: ArrayView1<f32>, scalar: f32) {
    Zip::from(&mut result_vec_u)
        .and(read_vec_u)
        .for_each(|result_val, read_val| {
            *result_val += scalar * read_val;
        })
}

struct GDSRandomGenerator {
    u: i64,
    v: i64,
    w: i64,
    seed: i64,
}

impl GDSRandomGenerator {
    const DOUBLE_PRECISION: i32 = 53;
    const DOUBLE_UNIT: f64 = f64::EPSILON * 0.5; // 2^(-53)
    fn new(seed: i64) -> Self {
        let mut x = GDSRandomGenerator {
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

fn rnd_vec_gds(
    write_vec: &mut [f32],
    node_seed: i64,
    random_seed: i64,
    degree: usize,
    _inv_degree_sum: f32,
    normalization_strength: f32,
) {
    const SPARSITY: usize = 3;
    const ENTRY_PROBABILITY: f64 = 1.0 / (2 * SPARSITY) as f64;

    let improved_random_seed = GDSRandomGenerator::new(random_seed).next_long();
    let mut random = GDSRandomGenerator::new(improved_random_seed ^ node_seed);
    let scaling = if degree == 0 {
        1.
    } else {
        (degree as f32).powf(normalization_strength)
    };
    let dim = write_vec.len();
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
                let deg = graph.out_degree(NI::new(u)).index();
                let inv_deg = if deg > 0 { 1f32 / deg as f32 } else { 1f32 };
                let read_vec_v = read_matrix.row(v.index());
                for (w_ud, r_ud) in write_vec_u.iter_mut().zip(read_vec_v.iter()) {
                    *w_ud += inv_deg * r_ud;
                }
            }
        });

    (read_matrix, write_matrix)
}

fn scale_vec_gds(result_vec_u: ArrayViewMut1<f32>, read_vec_u: ArrayView1<f32>, coef: f32) {
    let scalar = {
        let l2sqr = read_vec_u.map(|x| x.powi(2)).sum();
        let safe_inv_l2 = if l2sqr == 0. { 1. } else { 1. / l2sqr.sqrt() };
        coef * safe_inv_l2
    };
    scale_vec(result_vec_u, read_vec_u, scalar);
}

#[cfg(test)]
mod tests {
    use std::ops::Mul;
    use crate::fast_rp::{fast_rp, FastRPConfig};
    use crate::prelude::*;
    use approx::RelativeEq;
    use ndarray::{s, Array2};
    const TOLERANCE: f32 = 1e-6;
    const MAX_RELATIVE_TOLERANCE: f32 = 1e-4;

    #[test]
    fn test_coefficients_on_bipartite() {
        let gdl = "(a0), (a1), (a2), (a3), \
        (a0)-->(a1), (a1)-->(a0), (a2)-->(a3), (a3)-->(a2)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let result_1: Array2<f32> = fast_rp(
            &graph,
            FastRPConfig::new(8, vec![1.], 0., Some(0), None, false)
        );

        let result_2: Array2<f32> = fast_rp(
            &graph,
            FastRPConfig::new(8, vec![0., 0., 1.], 0., Some(0), None, false)
        );

        assert!(result_1.relative_eq(&result_2, TOLERANCE, MAX_RELATIVE_TOLERANCE));
    }

    #[test]
    fn test_normalization_strength() {
        let gdl_1 = "(a0), (a1), (a2), (a3), (a4)\
        (a0)-->(a1), (a1)-->(a0), (a2)-->(a3), (a3)-->(a2), (a4)-->(a4)";

        let gdl_2 = "(a0), (a1), (a2), (a3), (a4)\
        (a0)-->(a1), (a1)-->(a0), (a2)-->(a3), (a3)-->(a2), (a0)-->(a4)";

        let graph_1: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl_1)
            .build()
            .unwrap();

        let graph_2: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl_2)
            .build()
            .unwrap();

        let result_1: Array2<f32> = fast_rp(
            &graph_1,
            FastRPConfig::new(8, vec![1.], 0., Some(0), None, false)
        );

        let result_2: Array2<f32> = fast_rp(
            &graph_2,
            FastRPConfig::new(8, vec![1.], 1., Some(0), None, false)
        );

        assert!(result_1.row(0).relative_eq(&result_2.row(0).mul(10./2.), TOLERANCE, MAX_RELATIVE_TOLERANCE));
        assert!(result_1.slice(s![1..,..]).relative_eq(&result_2.slice(s![1..,..]).mul(10./1.), TOLERANCE, MAX_RELATIVE_TOLERANCE));
    }

    #[test]
    fn test_common_random_seed() {
        let gdl = "(a0),(a1),(a2),(a3),\
        (a0)-->(a1),(a1)-->(a0),(a0)-->(a2),(a3)-->(a2)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let config_42 = FastRPConfig::new(8, vec![0., 0., 1.], 0., Some(42), None, false);
        let config_rnd = FastRPConfig::new(8, vec![0., 0., 1.], 0., None, None, false);

        assert!(fast_rp(&graph, config_42.clone()).relative_eq(&fast_rp(&graph, config_42.clone()), TOLERANCE, MAX_RELATIVE_TOLERANCE));
        assert!(fast_rp(&graph, config_42.clone()).relative_ne(&fast_rp(&graph, config_rnd.clone()), TOLERANCE, MAX_RELATIVE_TOLERANCE)); //could fail if unlucky, hmm
    }

    #[test]
    fn test_node_random_seed() {
        let gdl_1 = "(a0),(a1),(a2),(a3),\
        (a0)-->(a1),(a1)-->(a0),(a0)-->(a2),(a3)-->(a2)";

        //gets node id based on order of definition
        let gdl_2 = "(a3),(a2),(a1),(a0),\
        (a0)-->(a1),(a1)-->(a0),(a0)-->(a2),(a3)-->(a2)";

        let graph_1: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl_1)
            .build()
            .unwrap();

        let graph_2: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl_2)
            .build()
            .unwrap();

        let node_seeds_1 = vec![100, 11, 22, 30];
        let node_seeds_2 = vec![30, 22, 11, 100];
        let result_1= fast_rp(&graph_1, FastRPConfig::new(8, vec![0., 0., 1.], 0., Some(42), Some(node_seeds_1), false));
        let result_2= fast_rp(&graph_2, FastRPConfig::new(8, vec![0., 0., 1.], 0., Some(42), Some(node_seeds_2), false));

        for (i,j) in (0..4).zip(4..0) {
            assert!(result_1.row(i).relative_eq(&result_2.row(j), TOLERANCE, MAX_RELATIVE_TOLERANCE));
        }

    }

    #[test]
    fn test_gds_consistency() {
        let gdl = "(a0),(a1),(a2),(a3),\
        (a0)-->(a1),(a1)-->(a0),(a0)-->(a2),(a3)-->(a2)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let result: Array2<f32> = fast_rp(
            &graph,
            FastRPConfig::new(8, vec![0., 0., 0., 1., 0.15], 0., Some(0), None, true),
        );

        let gds_result: Array2<f32> = Array2::from_shape_vec(
            (4, 8),
            vec![
                0.0,
                0.44015592336654663,
                -0.6205042600631714,
                0.3535533845424652,
                0.3535533845424652,
                0.0,
                0.08660253882408142,
                0.3535533845424652,
                0.0,
                0.6303832530975342,
                0.47128424048423767,
                0.0530330091714859,
                0.0530330091714859,
                0.0,
                0.5773502588272095,
                0.0530330091714859,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
        )
        .unwrap();

        assert!(result.relative_eq(&gds_result, TOLERANCE, MAX_RELATIVE_TOLERANCE));
    }
}
