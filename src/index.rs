use std::fmt::Debug;
use std::iter::{Step, Sum};
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
    + Sum
    + Sync
    + Sized
    + Step
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
    type Inner: Idx<Atomic = Self>;

    fn load(&self, order: Ordering) -> Self::Inner;

    fn fetch_add(&self, val: Self::Inner, order: Ordering) -> Self::Inner;

    fn get_and_increment(&self, order: Ordering) -> Self::Inner;

    fn zero() -> Self;

    fn into_inner(self) -> Self::Inner;
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
    fn get_and_increment(&self, order: Ordering) -> Self::Inner {
        self.fetch_add(1, order)
    }

    #[inline]
    fn zero() -> Self {
        AtomicUsize::new(0)
    }

    #[inline]
    fn into_inner(self) -> Self::Inner {
        self.into_inner()
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
    fn fetch_add(&self, val: u32, order: Ordering) -> Self::Inner {
        self.fetch_add(val, order)
    }

    #[inline]
    fn get_and_increment(&self, order: Ordering) -> Self::Inner {
        self.fetch_add(1, order)
    }

    #[inline]
    fn zero() -> Self {
        AtomicU32::new(0)
    }

    #[inline]
    fn into_inner(self) -> Self::Inner {
        self.into_inner()
    }
}
