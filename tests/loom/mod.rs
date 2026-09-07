#![allow(dead_code, unused_imports)]

#[cfg(all(loom, not(debug_assertions)))]
compile_error!("loom tests requires debug_assertions enabled");

#[cfg(not(loom))]
pub use std::{
    sync::atomic::{AtomicBool, AtomicUsize, fence},
    thread,
};

#[cfg(not(loom))]
pub use futures::executor::block_on;
#[cfg(loom)]
pub use loom::{
    future::block_on,
    model,
    sync::atomic::{AtomicBool, AtomicUsize, fence},
};

#[cfg(not(loom))]
pub fn model(f: impl Fn() + Sync + Send + 'static) {
    f();
}

#[cfg(loom)]
pub mod thread {
    use std::{
        cell::RefCell,
        marker::PhantomData,
        panic::{AssertUnwindSafe, catch_unwind},
    };

    use loom::thread::JoinHandle;
    pub use loom::thread::{spawn, yield_now};

    #[derive(Default)]
    pub struct Scope<'env> {
        handles: RefCell<Vec<Option<JoinHandle<std::thread::Result<()>>>>>,
        dummy: loom::sync::Arc<loom::sync::atomic::AtomicUsize>,
        _env: PhantomData<&'env mut ()>,
    }

    impl Drop for Scope<'_> {
        fn drop(&mut self) {
            for handle in self.handles.get_mut().drain(..).flatten() {
                if let Err(err) = handle.join().unwrap() {
                    std::panic::resume_unwind(err);
                }
            }
        }
    }

    impl<'env> Scope<'env> {
        pub fn spawn<T: Send + 'env>(&self, f: impl FnOnce() -> T + Send + 'env) {
            let mut handles = self.handles.borrow_mut();
            let dummy = self.dummy.clone();
            handles.push(Some(spawn(unsafe {
                core::mem::transmute::<
                    Box<dyn FnOnce() -> std::thread::Result<()> + Send + 'env>,
                    Box<dyn FnOnce() -> std::thread::Result<()> + Send + 'static>,
                >(Box::new(move || {
                    // https://github.com/tokio-rs/loom/issues/392
                    dummy.store(1, loom::sync::atomic::Ordering::Relaxed);
                    // https://github.com/tokio-rs/loom/issues/417
                    catch_unwind(AssertUnwindSafe(|| {
                        f();
                    }))
                }))
            })));
        }
    }

    pub fn scope<'env, T>(f: impl FnOnce(&Scope<'env>) -> T) -> T {
        let scope = Scope::default();
        scope.dummy.store(1, loom::sync::atomic::Ordering::Relaxed);
        f(&scope)
    }
}
