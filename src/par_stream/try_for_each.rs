use async_std::prelude::*;
use async_std::sync::{self, Receiver, Sender};
use async_std::task::{self, Context, Poll};

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::ParallelStream;

pin_project_lite::pin_project! {
    /// Call a closure on each element of the stream.
    #[derive(Debug)]
    pub struct TryForEach<E> {
        // Receiver that tracks whether all tasks have finished executing.
        #[pin]
        receiver: Receiver<Option<E>>,
        // Track whether the input stream has been exhausted.
        exhausted: Arc<AtomicBool>,
        // Count how many tasks are executing.
        ref_count: Arc<AtomicU64>,
        // determines if an error occured
        error_occurred: Arc<AtomicBool>
    }
}

impl<E: Send + 'static> TryForEach<E> {
    /// Create a new instance of `ForEach`.
    pub fn new<S, F, Fut>(mut stream: S, mut f: F) -> Self
        where
            S: ParallelStream,
            F: FnMut(S::Item) -> Fut + Send + Sync + Copy + 'static,
            Fut: Future<Output=Result<(), E>> + Send,
    {
        let exhausted = Arc::new(AtomicBool::new(false));
        let ref_count = Arc::new(AtomicU64::new(0));
        let error_occurred = Arc::new(AtomicBool::new(false));
        let (sender, receiver): (Sender<Option<E>>, Receiver<Option<E>>) = sync::channel(1);
        let _limit = stream.get_limit();

        // Initialize the return type here to prevent borrowing issues.
        let this = Self {
            receiver,
            exhausted: exhausted.clone(),
            ref_count: ref_count.clone(),
            error_occurred: error_occurred.clone(),
        };

        task::spawn(async move {
            while let Some(item) = stream.next().await {
                let sender = sender.clone();
                let exhausted = exhausted.clone();
                let ref_count = ref_count.clone();
                let error_occurred = error_occurred.clone();

                ref_count.fetch_add(1, Ordering::SeqCst);
                // if no error occured, we can spawn another task.
                // but, if an error occured, break from the stream, ending the outer task
                if error_occurred.load(Ordering::SeqCst) {
                    return;
                }

                task::spawn(async move {
                    // an error may have occured by the time the runtime executes this closure
                    if !error_occurred.load(Ordering::SeqCst) {
                        // Execute the closure.
                        if let Err(err) = f(item).await {
                            // an error occured. We need to stop any consequent futures from starting
                            // and then send an item through the stream
                            error_occurred.store(true, Ordering::SeqCst);
                            sender.send(Some(err)).await;
                            return;
                        }

                        // Wake up the receiver if we know we're done.
                        ref_count.fetch_sub(1, Ordering::SeqCst);
                        if exhausted.load(Ordering::SeqCst) && ref_count.load(Ordering::SeqCst) == 0 {
                            // possibly, an error occured while executing this inner task. Make sure no error occured
                            if !error_occurred.load(Ordering::SeqCst) {
                                // no errors occured at all, so push None through the channel
                                sender.send(None).await;
                            }
                        }
                    }
                });
            }

            // The input stream will no longer yield items.
            exhausted.store(true, Ordering::SeqCst);
        });

        this
    }
}

impl<E> Future for TryForEach<E> {
    type Output = Result<(), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Some(Some(err)) = task::ready!(this.receiver.poll_next(cx)) {
            Poll::Ready(Err(err))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

#[async_std::test]
async fn try_smoke_ok() {
    let s = async_std::stream::repeat(5usize);
    let res = crate::from_stream(s)
        .take(3)
        .try_for_each::<_, _, ()>(|n| async move {
            // TODO: assert that this is called 3 times.
            dbg!(n);
            Ok(())
        })
        .await;

    assert!(res.is_ok());
}

#[async_std::test]
async fn try_smoke_err() {
    let s = async_std::stream::successors(Some(0), |val| Some(val + 1));
    let res = crate::from_stream(s)
        .take(10)
        .try_for_each::<_, _, ()>(|n| async move {
            // TODO: assert that this is called 3 times.
            dbg!(n);
            if n != 3 {
                Ok(())
            } else {
                Err(())
            }
        })
        .await;

    assert!(res.is_err());
}