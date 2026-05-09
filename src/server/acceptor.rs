use std::{
    net::TcpListener,
    sync::{Arc, atomic::Ordering::Relaxed, mpsc},
};

use mio::net::TcpStream;
use mio_runtime::Waker;

use crate::stats::Stats;

pub struct Acceptor {
    listener: TcpListener,
    senders: Vec<mpsc::Sender<TcpStream>>,
    wakers: Vec<Waker>,
    num_workers: usize,
    stats: Arc<Stats>,
    max_connections: usize,
    next_worker: usize,
}

impl Acceptor {
    pub fn new(
        listener: TcpListener,
        senders: Vec<mpsc::Sender<TcpStream>>,
        wakers: Vec<Waker>,
        stats: Arc<Stats>,
        max_connections: usize,
    ) -> Acceptor {
        debug_assert_eq!(senders.len(), wakers.len());
        let num_workers = senders.len();
        Acceptor {
            listener,
            senders,
            wakers,
            num_workers,
            stats,
            max_connections,
            next_worker: 0,
        }
    }

    pub fn run(&mut self) {
        for incoming in self.listener.incoming() {
            match incoming {
                Ok(stream) => {
                    if self.stats.active_connections.load(Relaxed) >= self.max_connections as i64 {
                        continue;
                    }
                    if let Err(e) = stream.set_nonblocking(true) {
                        eprintln!("acceptor: set_nonblocking failed: {}", e);
                        continue;
                    }
                    if let Err(e) = stream.set_nodelay(true) {
                        eprintln!("acceptor: set_nodelay failed: {}", e);
                        continue;
                    }
                    let i = self.next_worker;
                    self.next_worker = (self.next_worker + 1) % self.num_workers;
                    let mio_stream = mio::net::TcpStream::from_std(stream);
                    if self.senders[i].send(mio_stream).is_err() {
                        eprintln!("acceptor: worker {i} sender closed; dropping connection");
                        continue;
                    }
                    if self.wakers[i].wake().is_err() {
                        eprintln!("acceptor: waker {i} closed; dropping connection");
                        continue;
                    }
                }
                Err(err) => {
                    eprintln!("Error accepting connection: {}", err);
                }
            }
        }
    }
}
