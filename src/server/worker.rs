use std::{
    io,
    sync::{Arc, mpsc::Receiver},
    time::Duration,
};

use mio::net::TcpStream;
use mio_runtime::{EventLoop, StopHandle, Waker};

use crate::{
    engine::StorageEngine,
    server::{CompactionCfg, WorkerHandler},
    stats::Stats,
};

pub struct Worker {
    pub event_loop: EventLoop,
    pub handler: WorkerHandler,
    pub waker: Waker,
    pub stop: StopHandle,
}

impl Worker {
    pub fn new(
        incoming: Receiver<TcpStream>,
        engine: Arc<dyn StorageEngine>,
        stats: Arc<Stats>,
        cfg: CompactionCfg,
        read_timeout_secs: Option<Duration>,
    ) -> io::Result<Self> {
        let event_loop = EventLoop::new(Duration::from_millis(512))?;
        let waker = event_loop.waker();
        let stop = event_loop.stop_handle();
        let handler = WorkerHandler::new(engine, stats, cfg, incoming, read_timeout_secs);
        Ok(Self {
            event_loop,
            handler,
            waker,
            stop,
        })
    }

    /// Consumes self, runs the event loop on the current thread.
    pub fn run(mut self) -> io::Result<()> {
        self.event_loop.run(&mut self.handler)
    }
}
