use std::{
    io::{self, ErrorKind, Read, Write},
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use crate::{
    bffp::{Command, decode_input_frame},
    record::{MAX_KEY_SIZE, MAX_VALUE_SIZE},
    stats::Stats,
};

const MAX_REQUEST_BYTES: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE + 1024;
const LEN_PREFIX_BYTES: usize = 4;
const READ_BUFFER_BYTES: usize = 4096;

enum ParseState {
    AwaitingLength,
    AwaitingBody(u32),
}

pub enum ConnectionAction {
    Continue, // keep the connection registered, wait for more events
    Close,    // deregister + drop after flushing any pending writes
}

pub struct Connection {
    pub stream: mio::net::TcpStream,
    pub parser: FrameParser,
    pub write_buf: Vec<u8>,     // pending bytes to send
    pub write_offset: usize,    // how much of write_buf is already sent
    pub last_activity: Instant, // for idle timeout sweep
    pub pending_close: bool, // set when readable signals Close; defer deregister until write_buf drains
    stats: Arc<Stats>,       // ties active_connections to Connection's lifetime via Drop
}

pub struct FrameParser {
    state: ParseState,
    buf: Vec<u8>,
}

impl FrameParser {
    pub fn new() -> Self {
        Self {
            state: ParseState::AwaitingLength,
            buf: Vec::new(),
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) -> io::Result<(Vec<Command>, ConnectionAction)> {
        self.buf.extend_from_slice(bytes);
        let mut parsed = Vec::new();
        let action = self.advance(&mut parsed)?;
        Ok((parsed, action))
    }

    fn advance(&mut self, parsed: &mut Vec<Command>) -> io::Result<ConnectionAction> {
        loop {
            match self.state {
                ParseState::AwaitingLength if self.buf.len() >= LEN_PREFIX_BYTES => {
                    let len_bytes: [u8; LEN_PREFIX_BYTES] = self.buf[..LEN_PREFIX_BYTES]
                        .try_into()
                        .expect("guard ensures slice has LEN_PREFIX_BYTES");
                    let len = u32::from_be_bytes(len_bytes);
                    if (len as usize) > MAX_REQUEST_BYTES {
                        return Ok(ConnectionAction::Close);
                    }
                    self.state = ParseState::AwaitingBody(len);
                }
                ParseState::AwaitingBody(len)
                    if self.buf.len() >= LEN_PREFIX_BYTES + len as usize =>
                {
                    let total = LEN_PREFIX_BYTES + len as usize;
                    let cmd = decode_input_frame(&self.buf[..total])?;
                    parsed.push(cmd);
                    self.buf.drain(..total);
                    self.state = ParseState::AwaitingLength;
                }
                _ => return Ok(ConnectionAction::Continue),
            }
        }
    }
}

impl Default for FrameParser {
    fn default() -> Self {
        Self::new()
    }
}

impl Connection {
    pub fn new(stream: mio::net::TcpStream, stats: Arc<Stats>) -> Self {
        stats.active_connections.fetch_add(1, Ordering::Relaxed);
        Self {
            stream,
            parser: FrameParser::new(),
            write_buf: Vec::new(),
            write_offset: 0,
            last_activity: Instant::now(),
            pending_close: false,
            stats,
        }
    }

    pub fn on_readable(&mut self) -> io::Result<(Vec<Command>, ConnectionAction)> {
        let mut all_parsed = Vec::new();
        let mut buf = [0u8; READ_BUFFER_BYTES];

        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => {
                    return Ok((all_parsed, ConnectionAction::Close));
                }
                Ok(n) => {
                    self.last_activity = Instant::now();
                    let (mut parsed, action) = self.parser.feed(&buf[..n])?;
                    all_parsed.append(&mut parsed);
                    if matches!(action, ConnectionAction::Close) {
                        return Ok((all_parsed, ConnectionAction::Close));
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Ok((all_parsed, ConnectionAction::Continue))
    }

    pub fn on_writable(&mut self) -> io::Result<()> {
        loop {
            if self.write_offset >= self.write_buf.len() {
                // fully drained
                self.write_buf.clear();
                self.write_offset = 0;
                return Ok(());
            }

            match self.stream.write(&self.write_buf[self.write_offset..]) {
                Ok(0) => break,
                Ok(n) => {
                    self.write_offset += n;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    pub fn enqueue_response(&mut self, bytes: &[u8]) {
        self.write_buf.extend_from_slice(bytes);
    }

    pub fn has_pending_writes(&self) -> bool {
        self.write_offset < self.write_buf.len()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.stats
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}
