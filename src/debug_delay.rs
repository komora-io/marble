/// This function is useful for inducing random jitter into
/// our atomic operations, shaking out more possible
/// interleavings quickly. It gets fully eliminated by the
/// compiler in non-test code.
pub fn debug_delay() {
    #[cfg(feature = "runtime_verification")]
    {
        use std::thread;
        use std::time::Duration;

        use rand::{thread_rng, Rng};

        let mut rng = thread_rng();

        match rng.gen_range(0..100) {
            0..=90 => thread::yield_now(),
            _ => thread::sleep(Duration::from_millis(5)),
        }
    }
}
