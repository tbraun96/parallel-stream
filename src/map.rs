// use async_std::prelude::*;
use async_std::future::Future;
use async_std::sync::{self, Receiver};
use async_std::task;

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::ParallelStream;

pin_project_lite::pin_project! {
    /// A parallel stream that maps value of another stream with a function.
    #[derive(Debug)]
    pub struct Map<T> {
        #[pin]
        receiver: Receiver<T>,
    }
}

impl<T: Send + 'static> Map<T> {
    /// Create a new instance of `Map`.
    pub fn new<S, F, Fut>(mut stream: S, mut f: F) -> Self
    where
        S: ParallelStream,
        F: FnMut(S::Item) -> Fut + Send + Sync + Copy + 'static,
        Fut: Future<Output = T> + Send,
    {
        let (sender, receiver) = sync::channel(1);
        task::spawn(async move {
            while let Some(item) = stream.next().await {
                let sender = sender.clone();
                task::spawn(async move {
                    let res = f(item).await;
                    sender.send(res).await;
                });
            }
        });
        Map { receiver }
    }
}

impl<T: Send + 'static> ParallelStream for Map<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use async_std::stream::Stream;
        let this = self.project();
        this.receiver.poll_next(cx)
    }
}

// #[async_std::test]
// async fn smoke() {
//     let s = async_std::stream::repeat(5usize).take(3);
//     let v: Vec<usize> = Map::new(s, |n| async move { n * 2 }).collect().await;
//     assert_eq!(v, vec![10usize; 3]);
// }
