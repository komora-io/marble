use std::sync::{Arc, Condvar, Mutex};

/// A value that is being computed in parallel.
pub struct Par<T: Send> {
    filler: Arc<Filler<T>>,
}

impl<T: Send> Par<T> {
    pub fn block(self) -> T {
        let mut mu = self.filler.mu.lock().unwrap();

        loop {
            if let Some(t) = mu.take() {
                return t;
            }

            mu = self.filler.cv.wait(mu).unwrap();
        }
    }
}

pub struct Filler<T: Send> {
    mu: Mutex<Option<T>>,
    cv: Condvar,
}

impl<T: Send> Filler<T> {
    fn fill(self, t: T) {
        let mut mu = self.mu.lock().unwrap();
        *mu = Some(t);
        drop(mu);

        self.cv.notify_all();
    }
}

pub fn pair<T: Send>() -> (Par<T>, Arc<Filler<T>>) {
    let filler = Arc::new(Filler {
        mu: Mutex::new(None),
        cv: Condvar::new(),
    });

    (
        Par {
            filler: filler.clone(),
        },
        filler,
    )
}
