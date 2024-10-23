use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use pin_project::pin_project;
use rand::Rng;
use tokio_stream::Stream;

enum State {
    Init,
    Sleeping,
}

#[pin_project]
pub struct DataAvailable {
    #[pin]
    sleep_future: Option<Pin<Box<tokio::time::Sleep>>>,
    state: State
}

impl DataAvailable {
    pub fn new() -> Self {
        Self {
            state: State::Init,
            sleep_future: None,
        }
    }

    fn random_delay() -> Duration {
        #[allow(unused_mut)]
        #[allow(unused_variables)]
        let mut rng = rand::thread_rng();
        let millis = rng.gen_range(0..=1000);
        Duration::from_millis(millis)
    }
}


impl Stream for DataAvailable {
    type Item = bool;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match *this.state {
                State::Init => {
                    let delay = Self::random_delay();
                    if let Some(ref mut sleep_future) = *this.sleep_future {
                        sleep_future
                            .as_mut()
                            .reset(tokio::time::Instant::now() + delay);
                    } else {
                        let sleep = tokio::time::sleep(delay);
                        this.sleep_future.set(Some(Box::pin(sleep)));
                    }
                    *this.state = State::Sleeping;
                },
                State::Sleeping => {
                    if let Some(sleep_future) = this.sleep_future.as_mut().as_pin_mut() {
                        match sleep_future.poll(cx) {
                            Poll::Ready(_) => {
                                *this.state = State::Init;
                                break Poll::Ready(Some(true));
                            }
                            Poll::Pending => {
                                break Poll::Pending;
                            }
                        }
                    } else {
                        break Poll::Pending;
                    }
                }
            }
        }
    }
}
