use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{sync_channel, Receiver, SendError, SyncSender};
use std::task::Waker;
use std::thread::{sleep, spawn};
use std::time::Duration;
use rand::distributions::uniform::SampleUniform;
use rand::{thread_rng, Rng};


// Define a trait for types that can be converted to and from bytes
pub trait ToBytes: Sized {
    fn to_le_bytes(&self) -> Vec<u8>;
    fn from_le_bytes(bytes: &[u8]) -> Self;
}


macro_rules! impl_tobytes_for {
    ($t:ty) => {
        impl ToBytes for $t {
            fn to_le_bytes(&self) -> Vec<u8> {
                Self::to_le_bytes(*self).to_vec()
            }

            fn from_le_bytes(bytes: &[u8]) -> Self {
                let array: [u8; std::mem::size_of::<Self>()] =
                    bytes.try_into().expect("slice with incorrect length");
                Self::from_le_bytes(array)
            }
        }
    };
}

// Implement ToBytes for some integer types
impl_tobytes_for!(i8);
impl_tobytes_for!(u64);
impl_tobytes_for!(i32);
impl_tobytes_for!(i16);
impl_tobytes_for!(u16);

pub trait Producer<T> {
    /// 产生一个新数据
    fn produce(&mut self) -> T;

    /// 校验生产者是否还有新数据
    fn data_available(&self) -> bool;

    /// 存储future的当前唤醒器
    fn store_waker(&mut self, waker: &Waker) {
        match self.get_waker() {
            None => {
                self.set_waker(Some(waker.clone()))
            },
            Some(old_waker) => {
                if !waker.will_wake(old_waker) {
                    println!("---------------------> Waker Changed");
                    self.set_waker(Some(waker.clone()))
                }
            }
        }
    }

    /// 给生产者发送停止信号
    fn stop(&mut self) {}

    fn set_waker(&mut self, waker: Option<Waker>);

    fn get_waker(&self) -> Option<&Waker>;
}

macro_rules! impl_waker_methods {
    () => {
        fn get_waker(&self) -> Option<&Waker> {
            self.waker.as_ref()
        }

        fn set_waker(&mut self, waker: Option<Waker>) {
            self.waker = waker;
        }
    };
}


/// 给异步收集器产生随机数
#[derive(Default)]
pub struct RandProducer<T> {
    /// Waker for waking up async tasks
    waker: Option<Waker>,
    /// Marker for generic type T
    _marker: std::marker::PhantomData<T>,
}

impl<T> Producer<T> for RandProducer<T>
where
    T: PartialOrd + From<u8> + SampleUniform
{
    fn produce(&mut self) -> T {
        let mut rng = thread_rng();
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }

        let r = std::ops::Range::<T> {
            start: T::from(1),
            end: T::from(10)
        };
        rng.gen_range(r)
    }

    fn data_available(&self) -> bool {
        let mut rng = thread_rng();
        sleep(Duration::from_millis(rng.gen_range(100..=1000)));
        true
    }

    impl_waker_methods!();
}


/// 通过通道发送数据的生产者
pub struct ChannelProducer<T> {
    waker: Option<Waker>,
    /// 发送数据到关联的线程
    sender: SyncSender<T>,
    /// 从其他线程接受数据
    receiver: Receiver<T>,
}

impl<T> Default for ChannelProducer<T> {
    fn default() -> Self {
        // bound为0, 表示发送方会阻塞, 直到接收方获取数据
        let (sender, receiver) = sync_channel::<T>(0);
        ChannelProducer {
            waker: None,
            sender,
            receiver,
        }
    }
}

impl<T> ChannelProducer<T>
where
    T: PartialOrd + From<u8> + SampleUniform + Send + 'static,
{
    #[allow(dead_code)]
    pub fn new() -> Self {
        let prod = Self::default();
        let sender = prod.sender.clone();
        spawn(move || loop {
            let mut rng = thread_rng();
            let r = std::ops::Range::<T> {
                start: T::from(1),
                end: T::from(100),
            };
            let val = rng.gen_range(r);
            match sender.send(val) {
                Ok(_) => continue,
                Err(_) => break
            }
        });
        prod
    }
}


impl<T> Producer<T> for ChannelProducer<T> {
    fn produce(&mut self) -> T {
        let data = self.receiver.recv().unwrap();
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
        data
    }

    fn data_available(&self) -> bool {
        let mut rng = thread_rng();
        sleep(Duration::from_millis(rng.gen_range(100..=1000)));
        true
    }

    impl_waker_methods!();
}


pub struct TCPProducer<T> {
    waker: Option<Waker>,
    stream: TcpStream,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TCPProducer<T>
where
    T: ToBytes + PartialOrd + From<u8> + SampleUniform
{
    const STOP: i8 = -1;
    const ACK: i8 = 0;

    #[allow(dead_code)]
    pub fn new(addr: impl Into<String>) -> Self {
        let addr: String = addr.into();
        let listener = TcpListener::bind(addr.clone()).expect("Build TCP listener");
        let server_handle = spawn(move || listener.accept().expect("Failed to accept connection"));

        // 生成一个线程通过 TCP 发送数据
        spawn({
            move || {
                if let Err(_e) = TCPProducer::<T>::send_data(addr) {}
            }
        });

        let (stream, _addr) = server_handle.join().unwrap();
        Self {
            waker: None,
            stream,
            _marker: std::marker::PhantomData,
        }
    }

    // Read data from the TCP stream
    fn read_data(&mut self) -> std::result::Result<T, std::io::Error> {
        write_number(&mut self.stream, TCPProducer::<T>::ACK)?; // Send ACK

        // Read exactly one value from the stream
        let ret = match read_number(&mut self.stream) {
            Ok(received_number) => received_number,
            Err(e) => {
                write_number(&mut self.stream, TCPProducer::<T>::STOP)?; // Send stop signal on error
                return Err(e);
            }
        };

        Ok(ret)
    }

    // Send data over the TCP stream in a loop
    fn send_data(addr: impl Into<String>) -> std::io::Result<()> {
        let addr: String = addr.into();
        let mut stream = TcpStream::connect(addr)?;
        let mut rng = thread_rng();

        loop {
            let r = std::ops::Range::<T> {
                start: T::from(0),
                end: T::from(250),
            };
            let number_to_send = rng.gen_range(r);

            let ret: i8 = match read_number(&mut stream) {
                Ok(received_number) => received_number,
                Err(e) => {
                    return Err(e);
                }
            };

            // Break the loop if stop signal is received
            if ret == TCPProducer::<T>::STOP {
                break;
            }

            // Send the generated number over the stream
            write_number(&mut stream, number_to_send)?;
        }
        Ok(())
    }
}


impl<T> Producer<T> for TCPProducer<T>
where
    T: ToBytes + From<u8> + SampleUniform + PartialOrd,
{
    // Produce data by reading from the TCP stream
    fn produce(&mut self) -> T {
        let data = self.read_data().unwrap();
        if let Some(waker) = &self.waker {
            waker.wake_by_ref(); // Wake up the async task if the waker is present
        }
        data
    }

    fn data_available(&self) -> bool {
        let mut rng = thread_rng();
        sleep(Duration::from_millis(rng.gen_range(100..=1000))); // Simulate a delay
        true
    }

    fn stop(&mut self) {
        let _ = write_number(&mut self.stream, TCPProducer::<T>::STOP); // Send stop signal to the client
    }

    impl_waker_methods!();
}


fn write_number<W: Write, T: ToBytes>(stream: &mut W, value: T) -> std::io::Result<()> {
    let bytes = value.to_le_bytes(); // Get the byte representation of the value
    stream.write_all(&bytes[..]) // Write all the bytes to the stream
}


fn read_number<R: Read, T: ToBytes>(stream: &mut R) -> std::io::Result<T> {
    let mut buf = vec![0u8; std::mem::size_of::<T>()]; // Create a buffer for the expected size
    stream.read_exact(&mut buf)?; // Read exactly the number of bytes needed
    Ok(T::from_le_bytes(&buf)) // Convert the byte array back into the original type
}





