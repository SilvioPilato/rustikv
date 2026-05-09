use std::{
    io,
    net::TcpListener,
    sync::{
        Arc,
        mpsc::{self},
    },
    thread::{self, JoinHandle, available_parallelism},
};

use crate::{
    engine::StorageEngine,
    server::{Acceptor, CompactionCfg, Worker},
    settings::Settings,
    stats::Stats,
};

pub struct Server {
    database: Arc<dyn StorageEngine>,
    settings: Settings,
    stats: Arc<Stats>,
}

impl Server {
    pub fn new(database: Arc<dyn StorageEngine>, settings: Settings, stats: Arc<Stats>) -> Self {
        Self {
            database,
            settings,
            stats,
        }
    }

    pub fn start(&self) -> io::Result<JoinHandle<()>> {
        let listener = TcpListener::bind(&self.settings.tcp_addr)?;
        let actual_addr = listener.local_addr()?;

        // Convert 0.0.0.0 to 127.0.0.1 for client connections
        let connect_addr = match actual_addr {
            std::net::SocketAddr::V4(addr) if addr.ip().is_unspecified() => {
                std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                    std::net::Ipv4Addr::LOCALHOST,
                    addr.port(),
                ))
            }
            std::net::SocketAddr::V6(addr) if addr.ip().is_unspecified() => {
                std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::LOCALHOST,
                    addr.port(),
                    0,
                    0,
                ))
            }
            _ => actual_addr,
        };

        // refactor this away in #73
        let addr_file = format!("{}/server.addr", self.settings.db_file_path);
        std::fs::write(&addr_file, connect_addr.to_string())?;

        let n_workers = self
            .settings
            .workers
            .unwrap_or(available_parallelism().map(|n| n.get()).unwrap_or(1));

        let compaction_cfg = CompactionCfg {
            ratio: self.settings.compaction_ratio,
            max_segment: self.settings.compaction_max_segment,
        };

        let mut senders = Vec::with_capacity(n_workers);
        let mut wakers = Vec::with_capacity(n_workers);

        for i in 0..n_workers {
            let (sender, receiver) = mpsc::channel();
            let worker = Worker::new(
                receiver,
                self.database.clone(),
                self.stats.clone(),
                compaction_cfg,
                self.settings.read_timeout_secs,
            )?;
            senders.push(sender);
            wakers.push(worker.waker.clone());

            thread::Builder::new()
                .name(format!("worker-{i}"))
                .spawn(move || {
                    if let Err(e) = worker.run() {
                        eprintln!("worker-{i} exited: {e}");
                    }
                })?;
        }

        let mut acceptor = Acceptor::new(
            listener,
            senders,
            wakers,
            self.stats.clone(),
            self.settings.max_connections,
        );

        let handle = thread::Builder::new()
            .name("acceptor".to_string())
            .spawn(move || {
                acceptor.run();
            })?;
        Ok(handle)
    }
}
