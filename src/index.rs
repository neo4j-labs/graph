use std::fmt::Debug;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use atoi::FromRadix10;

pub trait Idx:
    Copy
    + std::ops::Add<Output = Self>
    + std::ops::AddAssign
    + std::ops::Sub<Output = Self>
    + std::ops::Div<Output = Self>
    + std::ops::Mul<Output = Self>
    + Ord
    + Debug
    + Send
    + Sync
    + Sized
    + 'static
{
    type Atomic: AtomicIdx<Inner = Self>;

    fn new(idx: usize) -> Self;

    fn zero() -> Self;

    fn index(self) -> usize;

    fn atomic(self) -> Self::Atomic;

    fn parse(bytes: &[u8]) -> (Self, usize);
}

pub trait AtomicIdx: Send + Sync {
    type Inner: Idx;

    fn load(&self, order: Ordering) -> Self::Inner;

    fn fetch_add(&self, val: usize, order: Ordering) -> Self::Inner;

    fn store(&self, val: Self::Inner, order: Ordering);

    fn zero() -> Self;

    fn copied(&self) -> Self;

    fn add(&mut self, other: Self);

    fn add_ref(&mut self, other: &Self);
}

impl Idx for usize {
    type Atomic = AtomicUsize;

    #[inline]
    fn new(idx: usize) -> Self {
        idx
    }

    #[inline]
    fn zero() -> Self {
        0
    }
    #[inline]
    fn index(self) -> usize {
        self
    }

    #[inline]
    fn atomic(self) -> AtomicUsize {
        AtomicUsize::new(self)
    }

    #[inline]
    fn parse(bytes: &[u8]) -> (Self, usize) {
        FromRadix10::from_radix_10(bytes)
    }
}

impl AtomicIdx for AtomicUsize {
    type Inner = usize;

    #[inline]
    fn load(&self, order: Ordering) -> Self::Inner {
        self.load(order)
    }

    #[inline]
    fn fetch_add(&self, val: usize, order: Ordering) -> Self::Inner {
        self.fetch_add(val, order)
    }

    #[inline]
    fn store(&self, val: Self::Inner, order: Ordering) {
        self.store(val.index(), order)
    }

    #[inline]
    fn zero() -> Self {
        AtomicUsize::new(0)
    }

    #[inline]
    fn copied(&self) -> Self {
        AtomicUsize::new(self.load(Ordering::SeqCst))
    }

    #[inline]
    fn add(&mut self, other: Self) {
        *self.get_mut() += other.into_inner();
    }

    #[inline]
    fn add_ref(&mut self, other: &Self) {
        *self.get_mut() += other.load(Ordering::SeqCst);
    }
}

impl Idx for u32 {
    type Atomic = AtomicU32;

    #[inline]
    fn new(idx: usize) -> Self {
        assert!(idx <= u32::MAX as usize);
        idx as u32
    }

    #[inline]
    fn zero() -> Self {
        0
    }

    #[inline]
    fn index(self) -> usize {
        self as usize
    }

    #[inline]
    fn atomic(self) -> AtomicU32 {
        AtomicU32::new(self)
    }

    #[inline]
    fn parse(bytes: &[u8]) -> (Self, usize) {
        FromRadix10::from_radix_10(bytes)
    }
}

impl AtomicIdx for AtomicU32 {
    type Inner = u32;

    #[inline]
    fn load(&self, order: Ordering) -> Self::Inner {
        self.load(order)
    }

    #[inline]
    fn fetch_add(&self, val: usize, order: Ordering) -> Self::Inner {
        self.fetch_add(val as u32, order)
    }

    #[inline]
    fn store(&self, val: Self::Inner, order: Ordering) {
        self.store(val.index() as u32, order)
    }

    #[inline]
    fn zero() -> Self {
        AtomicU32::new(0)
    }

    #[inline]
    fn copied(&self) -> Self {
        AtomicU32::new(self.load(Ordering::SeqCst))
    }

    #[inline]
    fn add(&mut self, other: Self) {
        *self.get_mut() += other.into_inner();
    }

    #[inline]
    fn add_ref(&mut self, other: &Self) {
        *self.get_mut() += other.load(Ordering::SeqCst);
    }
}
