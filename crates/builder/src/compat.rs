use std::mem::MaybeUninit;

pub(crate) trait MaybeUninitWriteSliceExt<T> {
    fn write_slice_compat<'a>(this: &'a mut [MaybeUninit<T>], src: &[T]) -> &'a mut [T]
    where
        T: Copy;
}

#[cfg(not(has_maybe_uninit_write_slice))]
impl<T> MaybeUninitWriteSliceExt<T> for MaybeUninit<T> {
    fn write_slice_compat<'a>(this: &'a mut [MaybeUninit<T>], src: &[T]) -> &'a mut [T]
    where
        T: Copy,
    {
        // SAFETY: &[T] and &[MaybeUninit<T>] have the same layout
        let uninit_src: &[MaybeUninit<T>] = unsafe { std::mem::transmute(src) };

        this.copy_from_slice(uninit_src);

        // SAFETY: Valid elements have just been copied into `this` so it is initialized
        // SAFETY: similar to safety notes for `slice_get_ref`, but we have a
        // mutable reference which is also guaranteed to be valid for writes.
        unsafe { &mut *(this as *mut [MaybeUninit<T>] as *mut [T]) }
    }
}

#[cfg(has_maybe_uninit_write_slice)]
impl<T> MaybeUninitWriteSliceExt<T> for MaybeUninit<T> {
    fn write_slice_compat<'a>(this: &'a mut [MaybeUninit<T>], src: &[T]) -> &'a mut [T]
    where
        T: Copy,
    {
        MaybeUninit::write_slice(this, src)
    }
}

pub(crate) trait NewUninitExt<T> {
    fn new_uninit_slice_compat(len: usize) -> Box<[MaybeUninit<T>]>;

    unsafe fn assume_init_compat(self) -> Box<[T]>;
}

#[cfg(not(has_new_uninit))]
impl<T> NewUninitExt<T> for Box<[MaybeUninit<T>]> {
    fn new_uninit_slice_compat(len: usize) -> Box<[MaybeUninit<T>]> {
        use std::mem::ManuallyDrop;
        use std::slice::from_raw_parts_mut;

        let vec = Vec::<T>::with_capacity(len);
        let mut vec = ManuallyDrop::new(vec);

        unsafe {
            let slice = from_raw_parts_mut(vec.as_mut_ptr() as *mut MaybeUninit<T>, len);
            Box::from_raw(slice)
        }
    }

    unsafe fn assume_init_compat(self) -> Box<[T]> {
        unsafe { Box::from_raw(Box::into_raw(self) as *mut [T]) }
    }
}

#[cfg(has_new_uninit)]
impl<T> NewUninitExt<T> for Box<[MaybeUninit<T>]> {
    fn new_uninit_slice_compat(len: usize) -> Box<[MaybeUninit<T>]> {
        Box::<[T]>::new_uninit_slice(len)
    }

    unsafe fn assume_init_compat(self) -> Box<[T]> {
        unsafe { self.assume_init() }
    }
}
