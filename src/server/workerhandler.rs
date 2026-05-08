use mio::{Interest, net::TcpStream};
use mio_runtime::{EventHandler, ReadyState, Registry, TimerId, Token};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc::Receiver},
};

use crate::{
    engine::StorageEngine,
    server::{CompactionCfg, Connection, ConnectionAction, dispatch},
    stats::Stats,
};

pub struct WorkerHandler {
    incoming: Receiver<TcpStream>,
    conns: HashMap<Token, Connection>,
    engine: Arc<dyn StorageEngine>,
    stats: Arc<Stats>,
    cfg: CompactionCfg,
    next_token: usize,
}

impl WorkerHandler {
    pub fn new(
        engine: Arc<dyn StorageEngine>,
        stats: Arc<Stats>,
        cfg: CompactionCfg,
        incoming: Receiver<TcpStream>,
    ) -> Self {
        Self {
            engine,
            stats,
            cfg,
            incoming,
            conns: HashMap::new(),
            next_token: 0,
        }
    }
}

impl EventHandler for WorkerHandler {
    fn on_event(&mut self, registry: &Registry, token: Token, ready: ReadyState) {
        let Some(mut conn) = self.conns.remove(&token) else {
            return;
        };
        let mut hard_close = false;

        // Skip readable processing once we've started a graceful close — we no
        // longer trust the stream and have already deregistered READABLE interest.
        if ready.readable() && !conn.pending_close {
            match conn.on_readable() {
                Ok((cmds, action)) => {
                    for cmd in cmds {
                        let res = dispatch(cmd, &self.engine, &self.stats, &self.cfg);
                        conn.enqueue_response(&res);
                    }
                    if matches!(action, ConnectionAction::Close) {
                        conn.pending_close = true;
                    }
                }
                Err(_) => hard_close = true,
            }
        }

        // Single drain attempt covers both opportunistic write (after readable
        // enqueued bytes) and standalone writable readiness — has_pending_writes
        // is true in either case, so checking ready.writable() separately would
        // just call on_writable a second time on an already-drained buffer.
        if !hard_close && conn.has_pending_writes() && conn.on_writable().is_err() {
            hard_close = true;
        }

        // Drop the connection on either:
        //   - hard close (I/O error or peer reset): no point flushing
        //   - graceful close completed: pending_close set AND write_buf drained
        if hard_close || (conn.pending_close && !conn.has_pending_writes()) {
            if let Err(e) = registry.deregister(&mut conn.stream) {
                // Cosmetic — Connection's Drop closes the FD which auto-removes
                // from the kernel's epoll set. Log and proceed.
                eprintln!("worker: deregister failed (token={:?}): {}", token, e);
            }
            // active_connections decrement happens in Connection::Drop when conn falls out of scope
            return;
        }

        // Otherwise pick the right interest set:
        //   - graceful close in progress: WRITABLE only (drain, don't read more)
        //   - pending writes: READABLE | WRITABLE
        //   - idle: READABLE only (avoid busy-loop on empty write buffer)
        let interest = if conn.pending_close {
            Interest::WRITABLE
        } else if conn.has_pending_writes() {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };
        if registry
            .reregister(&mut conn.stream, token, interest)
            .is_err()
        {
            if let Err(e) = registry.deregister(&mut conn.stream) {
                eprintln!("worker: deregister failed (token={:?}): {}", token, e);
            }
            return;
        }

        self.conns.insert(token, conn);
    }

    fn on_timer(&mut self, _registry: &Registry, _timer_id: TimerId) {}

    fn on_wake(&mut self, registry: &Registry) {
        while let Ok(mut stream) = self.incoming.try_recv() {
            let token = Token(self.next_token);
            self.next_token += 1;
            if let Err(e) = registry.register(&mut stream, token, Interest::READABLE) {
                eprintln!("register failed: {e}");
                continue;
            }
            self.conns
                .insert(token, Connection::new(stream, self.stats.clone()));
        }
    }
}
