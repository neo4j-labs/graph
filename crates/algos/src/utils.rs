#[derive(Clone, Debug)]
/// An iterator adaptor that allows putting back a single
/// item to the front of the iterator.
///
/// Iterator element type is `I::Item`.
///
/// Code is copied from [itertools](https://docs.rs/itertools/latest/itertools/structs/struct.PutBack.html).
pub struct PutBack<I>
where
    I: Iterator,
{
    top: Option<I::Item>,
    iter: I,
}

pub fn put_back_iterator<I>(iterable: I) -> PutBack<I::IntoIter>
where
    I: IntoIterator,
{
    PutBack {
        top: None,
        iter: iterable.into_iter(),
    }
}
impl<I> PutBack<I>
where
    I: Iterator,
{
    /// put back value `value` (builder method)
    pub fn with_value(mut self, value: I::Item) -> Self {
        self.put_back(value);
        self
    }

    /// Split the `PutBack` into its parts.
    #[inline]
    pub fn into_parts(self) -> (Option<I::Item>, I) {
        let PutBack { top, iter } = self;
        (top, iter)
    }

    /// Put back a single value to the front of the iterator.
    ///
    /// If a value is already in the put back slot, it is overwritten.
    #[inline]
    pub fn put_back(&mut self, x: I::Item) {
        self.top = Some(x)
    }
}

impl<I> Iterator for PutBack<I>
where
    I: Iterator,
{
    type Item = I::Item;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.top {
            None => self.iter.next(),
            ref mut some => some.take(),
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // Not ExactSizeIterator because size may be larger than usize
        let x = self.top.is_some() as usize;
        let (mut low, mut hi) = self.iter.size_hint();
        low = low.saturating_add(x);
        hi = hi.and_then(|elt| elt.checked_add(x));
        (low, hi)
    }

    fn count(self) -> usize {
        self.iter.count() + (self.top.is_some() as usize)
    }

    fn last(self) -> Option<Self::Item> {
        self.iter.last().or(self.top)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.top {
            None => self.iter.nth(n),
            ref mut some => {
                if n == 0 {
                    some.take()
                } else {
                    *some = None;
                    self.iter.nth(n - 1)
                }
            }
        }
    }

    fn all<G>(&mut self, mut f: G) -> bool
    where
        G: FnMut(Self::Item) -> bool,
    {
        if let Some(elt) = self.top.take() {
            if !f(elt) {
                return false;
            }
        }
        self.iter.all(f)
    }

    fn fold<Acc, G>(mut self, init: Acc, mut f: G) -> Acc
    where
        G: FnMut(Acc, Self::Item) -> Acc,
    {
        let mut accum = init;
        if let Some(elt) = self.top.take() {
            accum = f(accum, elt);
        }
        self.iter.fold(accum, f)
    }
}
