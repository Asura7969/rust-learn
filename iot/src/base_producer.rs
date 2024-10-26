use std::pin::Pin;
use std::task::{Context, Poll};
use pin_project::pin_project;
use tokio::sync::mpsc::Receiver;
use tokio_stream::Stream;
use crate::stream::DataAvailable;

pub trait HasErrorValue {
    fn is_error_value(&self) -> bool;
    fn into_error_value(&self) -> Self;
}

impl HasErrorValue for i32 {
    fn is_error_value(&self) -> bool {
        *self == -1
    }

    fn into_error_value(&self) -> Self {
        -1
    }
}


#[pin_project]
pub struct Producer<T>
where
    T: HasErrorValue
{
    #[pin]
    receiver: Receiver<T>,

    post_process_fn: Box<dyn Fn(T) -> T + 'static + Send>,

    #[pin]
    data_available: DataAvailable,
}


impl<T> Producer<T>
where
    T: HasErrorValue
{
    pub fn new(
        data_available: DataAvailable,
        receiver: Receiver<T>,
        post_process_fn: Box<dyn Fn(T) -> T + Send + 'static>,
    ) -> Self {
        Self {
            data_available,
            receiver,
            post_process_fn,
        }
    }
}



impl<T> Stream for Producer<T>
where
    T: HasErrorValue
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        // First, poll the DataAvailable stream to check if data is ready to be processed
        match this.data_available.poll_next(cx) {
            // DataAvailable indicates data is ready
            Poll::Ready(Some(true)) => {
                // Poll the receiver (mpsc channel) to get the next data item
                match this.receiver.poll_recv(cx) {
                    // Data is available on the channel
                    Poll::Ready(Some(data)) => {
                        if data.is_error_value() {
                            // Sender informs us about an error condition
                            Poll::Ready(None)
                        } else {
                            // Apply the post-processing function to the data and return it
                            Poll::Ready(Some((this.post_process_fn)(data)))
                            // Return the processed data
                        }
                    }
                    Poll::Ready(None) => {
                        Poll::Ready(None) // Indicate that the stream has ended
                    }
                    // No data is available on the channel yet, return Poll::Pending
                    Poll::Pending => {
                        Poll::Pending // Still waiting for data, return Poll::Pending
                    }
                }
            }
            // DataAvailable indicates data is not yet available, return Poll::Pending
            Poll::Ready(Some(false)) => {
                Poll::Pending // Wait for data to become available
            }
            // DataAvailable is still in progress, waiting for the next cycle
            Poll::Pending => {
                Poll::Pending // DataAvailable is still pending, so we also return Poll::Pending
            }
            // DataAvailable stream has ended, no more items will be produced
            Poll::Ready(None) => {
                Poll::Ready(None) // End of stream
            }
         }
    }
}


mod test {
    use log::info;
    use tokio::sync::mpsc::channel;
    use tokio_stream::StreamExt;
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn my_test() {
        if let None = std::env::var_os("RUST_LOG") {
            std::env::set_var("RUST_LOG", "info")
        }
        env_logger::init();

        // Create the DataAvailable stream
        let data_available = DataAvailable::new();

        // Create an mpsc channel to send and receive data
        let (tx, rx) = channel(1);

        // Create the Producer stream with DataAvailable and a channel receiver
        let mut producer = Producer::new(data_available, rx, Box::new(|x: i32| x + 1));

        // Spawn a task to send data into the channel
        tokio::spawn(async move {
            for i in 0..=10 {
                tx.send(i).await.unwrap();
            }
        });

        // Consume the Producer stream and print the result
        while let Some(data) = producer.next().await {
            info!("Produced data: {}", data);
        }
    }

}






























