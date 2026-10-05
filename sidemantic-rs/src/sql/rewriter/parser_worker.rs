//! Reuse a large-stack parser worker per calling thread. Workers terminate when
//! their caller exits; independent callers never serialize behind one worker.

use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{self, Sender};

use polyglot_sql::Expression;

use crate::error::{Result, SidemanticError};

type Parse = Box<dyn FnOnce() -> Result<Vec<Expression>> + Send>;
struct Job {
    parse: Parse,
    result: Sender<Result<Vec<Expression>>>,
}

thread_local! {
    static WORKER: RefCell<Option<Sender<Job>>> = const { RefCell::new(None) };
    static ON_WORKER: Cell<bool> = const { Cell::new(false) };
}

fn guarded_parse(parse: impl FnOnce() -> Result<Vec<Expression>>) -> Result<Vec<Expression>> {
    catch_unwind(AssertUnwindSafe(parse))
        .map_err(|_| SidemanticError::SqlParse("Polyglot parser thread panicked".into()))?
}

pub(super) fn run(
    parse: impl FnOnce() -> Result<Vec<Expression>> + Send + 'static,
) -> Result<Vec<Expression>> {
    // Nested parser work is already on the large stack. Dispatching it back to
    // the same worker would deadlock; creating another worker is unnecessary.
    if ON_WORKER.get() {
        return guarded_parse(parse);
    }

    WORKER.with(|worker| {
        let mut worker = worker.borrow_mut();
        if worker.is_none() {
            let (sender, receiver) = mpsc::channel::<Job>();
            std::thread::Builder::new()
                .name("sidemantic-parser".into())
                .stack_size(16 * 1024 * 1024)
                .spawn(move || {
                    ON_WORKER.set(true);
                    for job in receiver {
                        // A malformed input must not kill the reusable worker.
                        let _ = job.result.send(guarded_parse(job.parse));
                    }
                })
                .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
            *worker = Some(sender);
        }

        let (result, receiver) = mpsc::channel();
        if worker
            .as_ref()
            .unwrap()
            .send(Job {
                parse: Box::new(parse),
                result,
            })
            .is_err()
        {
            *worker = None;
            return Err(SidemanticError::SqlParse("Parser worker stopped".into()));
        }
        match receiver.recv() {
            Ok(result) => result,
            Err(_) => {
                *worker = None;
                Err(SidemanticError::SqlParse("Parser worker stopped".into()))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn worker_id() -> std::thread::ThreadId {
        let (sender, receiver) = mpsc::channel();
        run(move || {
            sender.send(std::thread::current().id()).unwrap();
            Ok(vec![])
        })
        .unwrap();
        receiver.recv().unwrap()
    }

    #[test]
    fn reuses_worker_after_success_error_and_panic() {
        let id = worker_id();
        assert_ne!(id, std::thread::current().id());
        assert_eq!(worker_id(), id);
        let error = run(|| Err(SidemanticError::SqlParse("bad SQL".into()))).unwrap_err();
        assert!(error.to_string().contains("bad SQL"));
        let error = run(|| panic!("parser panic regression")).unwrap_err();
        assert!(error
            .to_string()
            .contains("Polyglot parser thread panicked"));
        assert_eq!(worker_id(), id);
    }

    #[test]
    fn nested_parser_calls_use_current_worker() {
        run(|| {
            assert_eq!(worker_id(), std::thread::current().id());
            Ok(vec![])
        })
        .unwrap();
    }

    #[test]
    fn independent_callers_parse_concurrently() {
        let (started, started_receiver) = mpsc::channel();
        let (release, release_receiver) = mpsc::channel();
        let blocked = std::thread::spawn(move || {
            run(move || {
                started.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(vec![])
            })
        });
        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let (finished, finished_receiver) = mpsc::channel();
        let independent = std::thread::spawn(move || {
            let result = super::super::parse_sql_with_large_stack("SELECT 1");
            finished
                .send(result.map(|statements| statements.len()))
                .unwrap();
        });
        let result = finished_receiver.recv_timeout(Duration::from_secs(5));
        // Always unblock the first worker, including when this regression fails.
        release.send(()).unwrap();
        blocked.join().unwrap().unwrap();
        independent.join().unwrap();
        assert_eq!(result.unwrap().unwrap(), 1);
    }
}
