#[cfg(no_maybe_uninit_write_slice)]
use std::mem::MaybeUninit;

#[cfg(no_maybe_uninit_write_slice)]
pub(crate) trait MaybeUninitWriteSliceExt<T> {
    fn write_slice<'a>(this: &'a mut [MaybeUninit<T>], src: &[T]) -> &'a mut [T]
    where
        T: Copy;
}

#[cfg(no_maybe_uninit_write_slice)]
impl<T> MaybeUninitWriteSliceExt<T> for MaybeUninit<T> {
    fn write_slice<'a>(this: &'a mut [MaybeUninit<T>], src: &[T]) -> &'a mut [T]
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
