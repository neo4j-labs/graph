use std::{mem::ManuallyDrop, sync::atomic::Ordering};

use rayon::prelude::*;

use crate::prelude::*;

/// A thread-safe Disjoint Set Struct implementation, that
/// can be safely shared and accessed across threads.
///
/// The implementation is based on the Java implementation [1]
/// which in turn is based on a C++ implementation and some
/// input from a Rust implementation [3].
///
/// This implementation is tailored for the graph crate as
/// it needs to support the `Idx` trait.
///
/// [1] [Java](https://github.com/neo4j/graph-data-science/blob/edeab6ab68f241135737aafe68a113248f11042c/core/src/main/java/org/neo4j/gds/core/utils/paged/dss/HugeAtomicDisjointSetStruct.java)
/// [2] [C++](https://github.com/wjakob/dset/blob/7967ef0e6041cd9d73b9c7f614ab8ae92e9e587a/dset.h)
/// [3] [Rust](https://github.com/tov/disjoint-sets-rs/blob/88ab08df21f04fcf7c157b6e042efd561ee873ba/src/concurrent.rs)
pub struct DisjointSetStruct<NI: Idx>(Box<[Atomic<NI>]>);

unsafe impl<NI: Idx> Sync for DisjointSetStruct<NI> {}
unsafe impl<NI: Idx> Send for DisjointSetStruct<NI> {}

impl<NI: Idx> UnionFind<NI> for DisjointSetStruct<NI> {
    /// Joins the set of `id1` with the set of `id2`.
    ///
    /// # Examples
    ///
    /// ```
    /// use graph::prelude::*;
    ///
    /// let dss = DisjointSetStruct::new(10);
    /// dss.union(2, 4);
    /// assert_eq!(dss.find(2), 2);
    /// assert_eq!(dss.find(4), 2);
    /// ```
    fn union(&self, mut id1: NI, mut id2: NI) {
        loop {
            id1 = self.find(id1);
            id2 = self.find(id2);

            if id1 == id2 {
                return;
            }

            // We do Union-by-Min, so the smaller set id wins.
            // We also only update the entry for id1 and if that
            // is the smaller value, we need to swap ids so we update
            // only the value for id2, not id1.
            if id1 < id2 {
                std::mem::swap(&mut id1, &mut id2);
            }

            let old_entry = id1;
            let new_entry = id2;

            if self.update_parent(id1, old_entry, new_entry).is_ok() {
                break;
            }
        }
    }

    /// Find the set of `id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use graph::prelude::*;
    ///
    /// let dss = DisjointSetStruct::new(10);
    /// assert_eq!(dss.find(4), 4);
    /// dss.union(4, 2);
    /// assert_eq!(dss.find(4), 2);
    /// ```
    fn find(&self, mut id: NI) -> NI {
        let mut parent = self.parent(id);

        while id != parent {
            let grand_parent = self.parent(parent);
            // Try to apply path-halving by setting the value
            // for some id to its grand parent. This might fail
            // if another thread is also changing the same value
            // but that's ok. The CAS operations guarantees
            // that at least one of the contenting threads will
            // succeed. That's enough for the path-halving to work
            // and there is no need to retry in case of a CAS failure.
            let _ = self.update_parent(id, parent, grand_parent);
            id = parent;
            parent = grand_parent;
        }

        id
    }

    /// Returns the number of elements in the dss, also referred to
    /// as its 'length'.
    ///
    /// # Examples
    ///
    /// ```
    /// use graph::prelude::*;
    ///
    /// let dss = DisjointSetStruct::<usize>::new(3);
    /// assert_eq!(dss.len(), 3);
    /// ```
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Compresses the DSS so that each id stores its root set id.
    fn compress(&self) {
        (0..self.len()).into_par_iter().map(NI::new).for_each(|id| {
            self.find(id);
        });
    }
}

impl<NI: Idx> DisjointSetStruct<NI> {
    /// Creates a new disjoint-set struct of `size` elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use graph::prelude::*;
    ///
    /// let dss = DisjointSetStruct::new(3);
    /// dss.union(0, 1);
    /// let set0 = dss.find(0);
    /// let set1 = dss.find(1);
    /// assert_eq!(set0, set1);
    /// ```
    pub fn new(size: usize) -> Self {
        let mut v = Vec::with_capacity(size);

        (0..size)
            .into_par_iter()
            .map(|i| Atomic::new(NI::new(i)))
            .collect_into_vec(&mut v);

        Self(v.into_boxed_slice())
    }

    fn parent(&self, i: NI) -> NI {
        self.0[i.index()].load(Ordering::SeqCst)
    }

    fn update_parent(&self, id: NI, current: NI, new: NI) -> Result<NI, NI> {
        self.0[id.index()].compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::Relaxed)
    }
}

impl<NI: Idx> Components<NI> for DisjointSetStruct<NI> {
    fn component(&self, node: NI) -> NI {
        self.find(node)
    }

    fn to_vec(self) -> Vec<NI> {
        let mut components = ManuallyDrop::new(self.0.into_vec());
        let (ptr, len, cap) = (
            components.as_mut_ptr(),
            components.len(),
            components.capacity(),
        );

        // SAFETY: NI and NI::Atomic have the same memory layout
        unsafe {
            let ptr = ptr as *mut Vec<NI>;
            let ptr = ptr as *mut _;
            Vec::from_raw_parts(ptr, len, cap)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use super::*;

    #[test]
    fn test_union() {
        let dss = DisjointSetStruct::new(10);

        assert_eq!(dss.find(9), 9);
        dss.union(9, 7);
        assert_eq!(dss.find(9), 7);
        dss.union(7, 4);
        assert_eq!(dss.find(9), 4);
        dss.union(4, 2);
        assert_eq!(dss.find(9), 2);
        dss.union(2, 0);
        assert_eq!(dss.find(9), 0);
    }

    #[test]
    fn test_union_with_path_halving() {
        let dss = DisjointSetStruct::new(10);

        dss.union(4, 3);
        dss.union(3, 2);
        dss.union(2, 1);
        dss.union(1, 0);

        dss.union(9, 8);
        dss.union(8, 7);
        dss.union(7, 6);
        dss.union(6, 5);

        assert_eq!(dss.find(4), 0);
        assert_eq!(dss.find(9), 5);

        dss.union(5, 4);

        for i in 0..dss.len() {
            assert_eq!(dss.find(i), 0);
        }
    }

    #[test]
    fn test_union_parallel() {
        let barrier = Arc::new(Barrier::new(2));
        let dss = Arc::new(DisjointSetStruct::new(1000));

        fn workload(barrier: &Barrier, dss: &DisjointSetStruct<u64>) {
            barrier.wait();
            for i in 0..500 {
                dss.union(i, i + 1);
            }
            // We wait again after the first cluster to increase the chance of concurrent updates
            barrier.wait();
            for i in 501..999 {
                dss.union(i, i + 1);
            }
        }

        let t1 = std::thread::spawn({
            let barrier = Arc::clone(&barrier);
            let dss = Arc::clone(&dss);
            move || workload(&barrier, &dss)
        });

        let t2 = std::thread::spawn({
            let barrier = Arc::clone(&barrier);
            let dss = Arc::clone(&dss);
            move || workload(&barrier, &dss)
        });

        t1.join().unwrap();
        t2.join().unwrap();

        for i in 0..500 {
            assert_eq!(dss.find(i), dss.find(i + 1));
        }

        assert_ne!(dss.find(500), dss.find(501));

        for i in 501..999 {
            assert_eq!(dss.find(i), dss.find(i + 1));
        }
    }
}
