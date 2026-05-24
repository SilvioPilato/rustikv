// Binary frame fixed protocol

use std::io::{self, Cursor, Read, Write};

const FLAGS_LEN: usize = 1;
const FRAME_LEN_SIZE: u64 = 4;
const OP_CODE_SIZE: usize = 1;
const KEY_LEN_SIZE: usize = 2;
const VALUE_LEN_SIZE: usize = 4;
const PAYLOAD_LEN_SIZE: usize = 4;
const TTL_LEN_SIZE: usize = 4;

const FLAG_HAS_TTL: u8 = 1;

pub enum Command {
    Invalid(u8),
    Read(String),
    Write(String, String, Option<u32>),
    Delete(String),
    Compact,
    Stats,
    List,
    Exists(String),
    Ping,
    Mget(Vec<String>),
    Mset(Vec<(String, String, Option<u32>)>),
    Range(String, String),
    Ttl(String, u32),
    Incr(String),
    Prefix(String),
    CountPrefix(String),
    CountRange(String, String),
}

#[repr(u8)]
pub enum OpCode {
    Read = 1,
    Write = 2,
    Delete = 3,
    Compact = 4,
    Stats = 5,
    List = 6,
    Exists = 7,
    Ping = 8,
    Mget = 9,
    Mset = 10,
    Range = 11,
    Ttl = 12,
    Incr = 13,
    Prefix = 14,
    CountPrefix = 15,
    CountRange = 16,
}

impl TryFrom<u8> for OpCode {
    type Error = io::Error;

    fn try_from(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(OpCode::Read),
            2 => Ok(OpCode::Write),
            3 => Ok(OpCode::Delete),
            4 => Ok(OpCode::Compact),
            5 => Ok(OpCode::Stats),
            6 => Ok(OpCode::List),
            7 => Ok(OpCode::Exists),
            8 => Ok(OpCode::Ping),
            9 => Ok(OpCode::Mget),
            10 => Ok(OpCode::Mset),
            11 => Ok(OpCode::Range),
            12 => Ok(OpCode::Ttl),
            13 => Ok(OpCode::Incr),
            14 => Ok(OpCode::Prefix),
            15 => Ok(OpCode::CountPrefix),
            16 => Ok(OpCode::CountRange),
            n => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown op code: {n}"),
            )),
        }
    }
}

#[repr(u8)]
pub enum ResponseStatus {
    Ok = 0,
    NotFound = 1,
    Error = 2,
    Noop = 3,
}

impl TryFrom<u8> for ResponseStatus {
    type Error = io::Error;

    fn try_from(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(ResponseStatus::Ok),
            1 => Ok(ResponseStatus::NotFound),
            2 => Ok(ResponseStatus::Error),
            3 => Ok(ResponseStatus::Noop),
            n => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown status byte: {n}"),
            )),
        }
    }
}

pub struct DecodedResponse {
    pub payload: Vec<String>,
    pub status: ResponseStatus,
}

pub fn decode_input_frame(buffer: &[u8]) -> io::Result<Command> {
    let mut cur = Cursor::new(buffer);

    let mut len_buf = [0u8; FRAME_LEN_SIZE as usize];
    cur.read_exact(&mut len_buf)?;
    let _total_len = u32::from_be_bytes(len_buf);

    let mut op_buf = [0u8; OP_CODE_SIZE];
    cur.read_exact(&mut op_buf)?;

    match OpCode::try_from(op_buf[0]) {
        Ok(OpCode::Read) => Ok(Command::Read(read_key(&mut cur)?)),
        Ok(OpCode::Write) => {
            let flag = read_flags(&mut cur)?;
            let key = read_key(&mut cur)?;
            let value = read_value(&mut cur)?;
            let ttl = if flag & FLAG_HAS_TTL != 0 {
                Some(read_ttl(&mut cur)?)
            } else {
                None
            };
            Ok(Command::Write(key, value, ttl))
        }
        Ok(OpCode::Delete) => Ok(Command::Delete(read_key(&mut cur)?)),
        Ok(OpCode::Compact) => Ok(Command::Compact),
        Ok(OpCode::Stats) => Ok(Command::Stats),
        Ok(OpCode::List) => Ok(Command::List),
        Ok(OpCode::Exists) => Ok(Command::Exists(read_key(&mut cur)?)),
        Ok(OpCode::Ping) => Ok(Command::Ping),
        Ok(OpCode::Mget) => {
            let mut items = Vec::new();
            while cur.position() < _total_len as u64 + FRAME_LEN_SIZE {
                let key = read_key(&mut cur)?;
                items.push(key);
            }

            Ok(Command::Mget(items))
        }
        Ok(OpCode::Mset) => {
            let mut items = Vec::new();
            while cur.position() < _total_len as u64 + FRAME_LEN_SIZE {
                let flag = read_flags(&mut cur)?;
                let key = read_key(&mut cur)?;
                let value = read_value(&mut cur)?;
                let ttl = if flag & FLAG_HAS_TTL != 0 {
                    Some(read_ttl(&mut cur)?)
                } else {
                    None
                };

                items.push((key, value, ttl));
            }

            Ok(Command::Mset(items))
        }
        Ok(OpCode::Range) => {
            let start = read_key(&mut cur)?;
            let end = read_key(&mut cur)?;
            Ok(Command::Range(start, end))
        }
        Ok(OpCode::Ttl) => {
            let key = read_key(&mut cur)?;
            let ttl = read_ttl(&mut cur)?;
            Ok(Command::Ttl(key, ttl))
        }
        Ok(OpCode::Incr) => Ok(Command::Incr(read_key(&mut cur)?)),
        Ok(OpCode::Prefix) => Ok(Command::Prefix(read_key(&mut cur)?)),
        Ok(OpCode::CountPrefix) => Ok(Command::CountPrefix(read_key(&mut cur)?)),
        Ok(OpCode::CountRange) => {
            let start = read_key(&mut cur)?;
            let end = read_key(&mut cur)?;
            Ok(Command::CountRange(start, end))
        }
        Err(_) => Ok(Command::Invalid(op_buf[0])),
    }
}

pub fn decode_response_frame(buffer: &[u8]) -> io::Result<DecodedResponse> {
    let mut responses: Vec<String> = Vec::new();
    let mut cur = Cursor::new(buffer);
    let mut len_buf = [0u8; FRAME_LEN_SIZE as usize];
    cur.read_exact(&mut len_buf)?;
    let total_len = u32::from_be_bytes(len_buf);
    let mut op_buf = [0u8; OP_CODE_SIZE];
    cur.read_exact(&mut op_buf)?;

    while cur.position() < total_len as u64 + FRAME_LEN_SIZE {
        let mut size_buf = [0u8; PAYLOAD_LEN_SIZE];
        cur.read_exact(&mut size_buf)?;
        let payload_size = u32::from_be_bytes(size_buf);
        let mut buf = vec![0u8; payload_size as usize];
        cur.read_exact(&mut buf)?;
        responses.push(
            String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );
    }

    Ok(DecodedResponse {
        payload: responses,
        status: ResponseStatus::try_from(op_buf[0])?,
    })
}

pub fn encode_frame(status: ResponseStatus, responses: &[String]) -> Vec<u8> {
    let mut payload = Cursor::new(Vec::new());
    payload.write_all(&[status as u8]).unwrap();
    for res in responses {
        let bytes = res.as_bytes();
        payload
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .unwrap();
        payload.write_all(bytes).unwrap();
    }

    let payload = payload.into_inner();
    let mut frame = Cursor::new(Vec::new());
    frame
        .write_all(&(payload.len() as u32).to_be_bytes())
        .unwrap();
    frame.write_all(&payload).unwrap();
    frame.into_inner()
}

pub fn encode_command(command: Command) -> Vec<u8> {
    let mut payload = Cursor::new(Vec::new());
    match command {
        Command::Read(key) => {
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + key.len()) as u32;
            let key_len = key.len() as u16;
            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::Read as u8]).unwrap();

            payload.write_all(&key_len.to_be_bytes()).unwrap();

            payload.write_all(key.as_bytes()).unwrap();

            payload.into_inner()
        }
        Command::Write(key, value, ttl) => {
            // | total_len(4) | OpCode::Write(1) | flags(1) | key_len(2) | key
            // | value_len(4) | value | [seconds(4) if flags & HAS_TTL] |
            let (flags, seconds) = match ttl {
                Some(ttl_secs) => (FLAG_HAS_TTL, Some(ttl_secs)),
                None => (0u8, None),
            };
            let ttl_bytes = if seconds.is_some() { TTL_LEN_SIZE } else { 0 };
            let total_len = (OP_CODE_SIZE
                + FLAGS_LEN
                + KEY_LEN_SIZE
                + VALUE_LEN_SIZE
                + key.len()
                + value.len()
                + ttl_bytes) as u32;
            let key_len = key.len() as u16;
            let val_len = value.len() as u32;

            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::Write as u8]).unwrap();

            payload.write_all(&flags.to_be_bytes()).unwrap();

            payload.write_all(&key_len.to_be_bytes()).unwrap();

            payload.write_all(key.as_bytes()).unwrap();

            payload.write_all(&val_len.to_be_bytes()).unwrap();

            payload.write_all(value.as_bytes()).unwrap();

            if let Some(secs) = seconds {
                payload.write_all(&secs.to_be_bytes()).unwrap();
            }

            payload.into_inner()
        }
        Command::Delete(key) => {
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + key.len()) as u32;
            let key_len = key.len() as u16;

            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::Delete as u8]).unwrap();

            payload.write_all(&key_len.to_be_bytes()).unwrap();

            payload.write_all(key.as_bytes()).unwrap();

            payload.into_inner()
        }
        Command::Compact => {
            let total_len = OP_CODE_SIZE as u32;
            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::Compact as u8]).unwrap();

            payload.into_inner()
        }
        Command::Stats => {
            let total_len = OP_CODE_SIZE as u32;
            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::Stats as u8]).unwrap();

            payload.into_inner()
        }
        Command::List => {
            let total_len = OP_CODE_SIZE as u32;
            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::List as u8]).unwrap();

            payload.into_inner()
        }
        Command::Exists(key) => {
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + key.len()) as u32;
            let key_len = key.len() as u16;

            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::Exists as u8]).unwrap();

            payload.write_all(&key_len.to_be_bytes()).unwrap();

            payload.write_all(key.as_bytes()).unwrap();

            payload.into_inner()
        }
        Command::Ping => {
            let total_len = OP_CODE_SIZE as u32;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::Ping as u8]).unwrap();
            payload.into_inner()
        }
        Command::Invalid(_) => unreachable!("Invalid is a decode-only sentinel, never encoded"),
        Command::Mget(keys) => {
            let mut total_len = OP_CODE_SIZE + KEY_LEN_SIZE * keys.len();
            for key in &keys {
                total_len += key.len();
            }

            payload
                .write_all(&(total_len as u32).to_be_bytes())
                .unwrap();
            payload.write_all(&[OpCode::Mget as u8]).unwrap();

            for key in keys {
                let key_len = key.len() as u16;
                payload.write_all(&key_len.to_be_bytes()).unwrap();
                payload.write_all(key.as_bytes()).unwrap();
            }

            payload.into_inner()
        }
        Command::Mset(items) => {
            // | total_len(4) | OpCode::Mset(1) | [ flags(1) | key_len(2) | key
            // | value_len(4) | value | [seconds(4) if flags & HAS_TTL] ]*
            let mut total_len =
                OP_CODE_SIZE + (FLAGS_LEN + KEY_LEN_SIZE + VALUE_LEN_SIZE) * items.len();
            for (k, v, ttl) in &items {
                total_len += k.len() + v.len();
                if ttl.is_some() {
                    total_len += TTL_LEN_SIZE;
                }
            }

            payload
                .write_all(&(total_len as u32).to_be_bytes())
                .unwrap();
            payload.write_all(&[OpCode::Mset as u8]).unwrap();

            for (k, v, ttl) in items {
                let (flags, seconds) = match ttl {
                    Some(ttl_secs) => (FLAG_HAS_TTL, Some(ttl_secs)),
                    None => (0u8, None),
                };
                payload.write_all(&flags.to_be_bytes()).unwrap();

                let key_len = k.len() as u16;
                payload.write_all(&key_len.to_be_bytes()).unwrap();
                payload.write_all(k.as_bytes()).unwrap();
                let val_len = v.len() as u32;
                payload.write_all(&val_len.to_be_bytes()).unwrap();
                payload.write_all(v.as_bytes()).unwrap();

                if let Some(secs) = seconds {
                    payload.write_all(&secs.to_be_bytes()).unwrap();
                }
            }

            payload.into_inner()
        }
        Command::Range(start, end) => {
            let start_len = start.len() as u16;
            let end_len = end.len() as u16;
            let total_len =
                OP_CODE_SIZE as u32 + KEY_LEN_SIZE as u32 * 2 + start_len as u32 + end_len as u32;
            payload.write_all(&(total_len).to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::Range as u8]).unwrap();
            payload.write_all(&start_len.to_be_bytes()).unwrap();
            payload.write_all(start.as_bytes()).unwrap();
            payload.write_all(&end_len.to_be_bytes()).unwrap();
            payload.write_all(end.as_bytes()).unwrap();

            payload.into_inner()
        }
        Command::Ttl(key, expiry) => {
            // | total_len(4) | OpCode::Ttl(1) | key_len(2) | key | seconds(4) |
            let key_len = key.len() as u16;
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + key.len() + TTL_LEN_SIZE) as u32;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::Ttl as u8]).unwrap();
            payload.write_all(&key_len.to_be_bytes()).unwrap();
            payload.write_all(key.as_bytes()).unwrap();
            payload.write_all(&expiry.to_be_bytes()).unwrap();

            payload.into_inner()
        }
        Command::Incr(key) => {
            // | total_len(4) | OpCode::Incr(1) | key_len(2) | key |
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + key.len()) as u32;
            let key_len = key.len() as u16;

            payload.write_all(&total_len.to_be_bytes()).unwrap();

            payload.write_all(&[OpCode::Incr as u8]).unwrap();

            payload.write_all(&key_len.to_be_bytes()).unwrap();

            payload.write_all(key.as_bytes()).unwrap();

            payload.into_inner()
        }
        Command::Prefix(prefix) => {
            // | total_len(4) | OpCode::Prefix(1) | key_len(2) | prefix |
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
            let key_len = prefix.len() as u16;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::Prefix as u8]).unwrap();
            payload.write_all(&key_len.to_be_bytes()).unwrap();
            payload.write_all(prefix.as_bytes()).unwrap();
            payload.into_inner()
        }
        Command::CountPrefix(prefix) => {
            // | total_len(4) | OpCode::CountPrefix(1) | key_len(2) | prefix |
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
            let key_len = prefix.len() as u16;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::CountPrefix as u8]).unwrap();
            payload.write_all(&key_len.to_be_bytes()).unwrap();
            payload.write_all(prefix.as_bytes()).unwrap();
            payload.into_inner()
        }
        Command::CountRange(start, end) => {
            // | total_len(4) | OpCode::CountRange(1) | start_len(2) | start | end_len(2) | end |
            let start_len = start.len() as u16;
            let end_len = end.len() as u16;
            let total_len =
                OP_CODE_SIZE as u32 + KEY_LEN_SIZE as u32 * 2 + start_len as u32 + end_len as u32;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::CountRange as u8]).unwrap();
            payload.write_all(&start_len.to_be_bytes()).unwrap();
            payload.write_all(start.as_bytes()).unwrap();
            payload.write_all(&end_len.to_be_bytes()).unwrap();
            payload.write_all(end.as_bytes()).unwrap();
            payload.into_inner()
        }
    }
}

fn read_string(cur: &mut Cursor<&[u8]>, len_bytes: usize) -> io::Result<String> {
    let len = match len_bytes {
        KEY_LEN_SIZE => {
            let mut b = [0u8; KEY_LEN_SIZE];
            cur.read_exact(&mut b)?;
            u16::from_be_bytes(b) as usize
        }
        VALUE_LEN_SIZE => {
            let mut b = [0u8; VALUE_LEN_SIZE];
            cur.read_exact(&mut b)?;
            u32::from_be_bytes(b) as usize
        }
        _ => unreachable!(),
    };
    let mut buf = vec![0u8; len];
    cur.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_key(cur: &mut Cursor<&[u8]>) -> io::Result<String> {
    read_string(cur, KEY_LEN_SIZE)
}

fn read_value(cur: &mut Cursor<&[u8]>) -> io::Result<String> {
    read_string(cur, VALUE_LEN_SIZE)
}

fn read_ttl(cur: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut b = [0u8; TTL_LEN_SIZE];
    cur.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_flags(cur: &mut Cursor<&[u8]>) -> io::Result<u8> {
    let mut b = [0u8; FLAGS_LEN];
    cur.read_exact(&mut b)?;
    Ok(u8::from_be_bytes(b))
}
