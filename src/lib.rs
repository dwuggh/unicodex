#![feature(impl_trait_in_assoc_type)]

pub mod app;
pub mod config;
pub mod ledger;
pub mod logging;
pub mod pricing;
pub mod proxy;

// Count allocations only on the measuring test's thread. Production uses its normal allocator.
#[cfg(test)]
pub(crate) mod allocation {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };

    thread_local! {
        static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
    }

    struct CountingAllocator;

    fn allocated() {
        let _ = COUNT.try_with(|count| {
            if let Some(value) = count.get() {
                count.set(Some(value + 1));
            }
        });
    }

    // SAFETY: All memory operations forward the caller's pointer and layout unchanged to System.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            allocated();
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            allocated();
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            allocated();
            unsafe { System.realloc(ptr, layout, size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    pub(crate) fn measure<R>(f: impl FnOnce() -> R) -> (R, usize) {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                COUNT.with(|count| count.set(None));
            }
        }
        COUNT.with(|count| {
            assert!(count.get().is_none(), "nested allocation measurement");
            count.set(Some(0));
        });
        let reset = Reset;
        let result = f();
        let count = COUNT.with(|count| count.get().unwrap());
        drop(reset);
        (result, count)
    }
}
