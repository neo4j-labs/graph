use std::sync::atomic::Ordering;

use rayon::prelude::*;

use crate::prelude::*;

/// A union find data structure based on [1].
///
/// Note, that this data structure requires calling `compress`
/// before calling `find` in order to return the correct set id.
///
/// [1]  Michael Sutton, Tal Ben-Nun, Amnon Barak:
///      "Optimizing Parallel Graph Connectivity Computation via Subgraph Sampling",
///       Symposium on Parallel and Distributed Processing, IPDPS 2018
pub struct Afforest<NI: Idx>(Box<[NI::Atomic]>);

unsafe impl<NI: Idx> Send for Afforest<NI> {}
unsafe impl<NI: Idx> Sync for Afforest<NI> {}

impl<NI: Idx> UnionFind<NI> for Afforest<NI> {
    // Corresponds to the `link` method described in [1].
    fn union(&self, u: NI, v: NI) {
        let mut p1 = self.find(u);
        let mut p2 = self.find(v);

        while p1 != p2 {
            let high = NI::max(p1, p2);
            let low = p1 + (p2 - high);
            let p_high = self.find(high);

            if p_high == low
                || (p_high == high && self.update_parent(self.find(high), high, low).is_ok())
            {
                break;
            }
            p1 = self.parent(self.parent(high));
            p2 = self.parent(low);
        }
    }

    fn find(&self, u: NI) -> NI {
        self.parent(u)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    // Corresponds to the `compress` method described in [1].
    fn compress(&self) {
        (0..self.len()).into_par_iter().map(NI::new).for_each(|n| {
            while self.parent(n) != self.parent(self.parent(n)) {
                self.0[n.index()].store(self.parent(self.parent(n)), Ordering::SeqCst)
            }
        });
    }
}

impl<NI: Idx> Afforest<NI> {
    /// Creates a new disjoint-set struct of `size` elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use graph::prelude::*;
    ///
    /// let af = Afforest::new(3);
    /// af.union(0, 1);
    /// af.compress();
    ///
    /// let set0 = af.find(0);
    /// let set1 = af.find(1);
    /// assert_eq!(set0, set1);
    /// ```
    pub fn new(size: usize) -> Self {
        let mut v = Vec::with_capacity(size);

        (0..size)
            .into_par_iter()
            .map(|i| NI::new(i).atomic())
            .collect_into_vec(&mut v);

        Self(v.into_boxed_slice())
    }

    #[inline]
    fn parent(&self, i: NI) -> NI {
        unsafe { self.0.get_unchecked(i.index()) }.load(Ordering::SeqCst)
    }

    #[inline]
    fn update_parent(&self, id: NI, current: NI, new: NI) -> Result<NI, NI> {
        unsafe { self.0.get_unchecked(id.index()) }.compare_exchange_weak(
            current,
            new,
            Ordering::SeqCst,
            Ordering::Relaxed,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_union() {
        let af = Afforest::new(10);

        af.union(9, 7);
        af.union(7, 4);
        af.union(4, 2);
        af.union(2, 0);

        af.compress();

        assert_eq!(af.find(9), 0);
    }
}
