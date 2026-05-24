use std::time::{SystemTime, UNIX_EPOCH};

pub fn is_expired(expiry_ms: u64, now_ms: u64) -> bool {
    expiry_ms <= now_ms
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_millis() as u64
}

/// Next Unicode scalar after `c`, skipping the surrogate gap. `None` for char::MAX.
fn next_scalar(c: char) -> Option<char> {
    match c {
        '\u{D7FF}' => Some('\u{E000}'), // jump the surrogate gap U+D800..=U+DFFF
        '\u{10FFFF}' => None,           // char::MAX — no successor
        _ => char::from_u32(c as u32 + 1), // guaranteed Some for all other scalars
    }
}

/// Least string strictly greater than every string starting with `prefix`.
/// `None` => no finite successor exists (empty prefix, or trailing char::MAX),
/// meaning "scan everything".
pub fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = next_scalar(last) {
            let mut s: String = chars.iter().collect();
            s.push(next);
            return Some(s);
        }
        // last was char::MAX: drop it and try to bump the preceding scalar.
    }
    None
}
