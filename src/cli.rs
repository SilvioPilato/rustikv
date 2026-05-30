use crate::bffp::Command;

pub enum ParseResult {
    Cmd(Command),
    Quit,
    InvalidInput(String),
}

pub fn parse_command(line: &str) -> ParseResult {
    let words: Vec<&str> = line.split_whitespace().collect();
    if words.is_empty() {
        return ParseResult::InvalidInput(String::new());
    }

    match words[0].to_uppercase().as_str() {
        "WRITE" => {
            if words.len() > 2 {
                ParseResult::Cmd(Command::Write(
                    words[1].to_string(),
                    words[2..].join(" "),
                    None,
                ))
            } else {
                ParseResult::InvalidInput("Usage: WRITE <key> <value>".to_string())
            }
        }
        "READ" => {
            if words.len() == 2 {
                ParseResult::Cmd(Command::Read(words[1].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: READ <key>".to_string())
            }
        }
        "DELETE" => {
            if words.len() == 2 {
                ParseResult::Cmd(Command::Delete(words[1].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: DELETE <key>".to_string())
            }
        }
        "EXISTS" => {
            if words.len() == 2 {
                ParseResult::Cmd(Command::Exists(words[1].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: EXISTS <key>".to_string())
            }
        }
        "INCR" => {
            if words.len() == 2 {
                ParseResult::Cmd(Command::Incr(words[1].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: INCR <key>".to_string())
            }
        }
        "USE" => {
            if words.len() == 2 {
                ParseResult::Cmd(Command::Use(words[1].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: USE <collection>".to_string())
            }
        }
        "CREATE" => {
            if words.len() >= 3 && words[1].eq_ignore_ascii_case("COLLECTION") {
                let name = words[2].to_string();
                match words.get(3) {
                    None => ParseResult::Cmd(Command::CreateCollection(name, None)),
                    Some(ttl_str) => match ttl_str.parse::<u32>() {
                        Ok(ttl) => ParseResult::Cmd(Command::CreateCollection(name, Some(ttl))),
                        Err(_) => ParseResult::InvalidInput("Invalid default TTL".to_string()),
                    },
                }
            } else {
                ParseResult::InvalidInput(
                    "Usage: CREATE COLLECTION <name> [default_ttl_secs]".to_string(),
                )
            }
        }
        "DROP" => {
            if words.len() == 3 && words[1].eq_ignore_ascii_case("COLLECTION") {
                ParseResult::Cmd(Command::DropCollection(words[2].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: DROP COLLECTION <name>".to_string())
            }
        }
        "SHOW" => {
            if words.len() == 2 && words[1].eq_ignore_ascii_case("COLLECTIONS") {
                ParseResult::Cmd(Command::ShowCollections)
            } else {
                ParseResult::InvalidInput("Usage: SHOW COLLECTIONS".to_string())
            }
        }
        "COMPACT" => ParseResult::Cmd(Command::Compact),
        "STATS" => ParseResult::Cmd(Command::Stats),
        "LIST" => ParseResult::Cmd(Command::List),
        "PING" => ParseResult::Cmd(Command::Ping),
        "QUIT" => ParseResult::Quit,
        "MGET" => {
            if words.len() < 2 {
                ParseResult::InvalidInput("Usage: MGET <key1> <key2> <keyn>".to_string())
            } else {
                let keys: Vec<String> = words[1..].iter().map(|k| k.to_string()).collect();
                ParseResult::Cmd(Command::Mget(keys))
            }
        }
        "MSET" => {
            if words.len() < 3 {
                ParseResult::InvalidInput("Usage: MSET <key1> <value1> <keyn> <valuen>".to_string())
            } else {
                let pairs: Vec<(String, String, Option<u32>)> = words[1..]
                    .chunks_exact(2)
                    .filter_map(|chunk| {
                        if let [k, v] = chunk {
                            Some((k.to_string(), v.to_string(), None))
                        } else {
                            None
                        }
                    })
                    .collect();
                ParseResult::Cmd(Command::Mset(pairs))
            }
        }
        "RANGE" => {
            if words.len() == 3 {
                ParseResult::Cmd(Command::Range(words[1].to_string(), words[2].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: RANGE <start> <end>".to_string())
            }
        }
        "PREFIX" => {
            if words.len() == 2 {
                ParseResult::Cmd(Command::Prefix(words[1].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: PREFIX <prefix>".to_string())
            }
        }
        "COUNT" => match words.len() {
            2 => ParseResult::Cmd(Command::CountPrefix(words[1].to_string())),
            3 => ParseResult::Cmd(Command::CountRange(
                words[1].to_string(),
                words[2].to_string(),
            )),
            _ => {
                ParseResult::InvalidInput("Usage: COUNT <prefix> | COUNT <start> <end>".to_string())
            }
        },
        "SUM" => match words.len() {
            2 => ParseResult::Cmd(Command::SumPrefix(words[1].to_string())),
            3 => ParseResult::Cmd(Command::SumRange(
                words[1].to_string(),
                words[2].to_string(),
            )),
            _ => ParseResult::InvalidInput("Usage: SUM <prefix> | SUM <start> <end>".to_string()),
        },
        "AVG" => match words.len() {
            2 => ParseResult::Cmd(Command::AvgPrefix(words[1].to_string())),
            3 => ParseResult::Cmd(Command::AvgRange(
                words[1].to_string(),
                words[2].to_string(),
            )),
            _ => ParseResult::InvalidInput("Usage: AVG <prefix> | AVG <start> <end>".to_string()),
        },
        "MIN" => match words.len() {
            2 => ParseResult::Cmd(Command::MinPrefix(words[1].to_string())),
            3 => ParseResult::Cmd(Command::MinRange(
                words[1].to_string(),
                words[2].to_string(),
            )),
            _ => ParseResult::InvalidInput("Usage: MIN <prefix> | MIN <start> <end>".to_string()),
        },
        "MAX" => match words.len() {
            2 => ParseResult::Cmd(Command::MaxPrefix(words[1].to_string())),
            3 => ParseResult::Cmd(Command::MaxRange(
                words[1].to_string(),
                words[2].to_string(),
            )),
            _ => ParseResult::InvalidInput("Usage: MAX <prefix> | MAX <start> <end>".to_string()),
        },
        "TTL" => {
            if words.len() == 3 {
                match words[2].parse::<u32>() {
                    Ok(expiry) => ParseResult::Cmd(Command::Ttl(words[1].to_string(), expiry)),
                    Err(_) => ParseResult::InvalidInput("Invalid expiry time".to_string()),
                }
            } else {
                ParseResult::InvalidInput("Usage: TTL <key> <seconds>".to_string())
            }
        }
        "WRITETTL" => {
            if words.len() >= 4 {
                match words[2].parse::<u32>() {
                    Ok(0) => ParseResult::InvalidInput(
                        "Usage: WRITETTL <key> <seconds> <value> (seconds must be >= 1)"
                            .to_string(),
                    ),
                    Ok(expiry) => ParseResult::Cmd(Command::Write(
                        words[1].to_string(),
                        words[3..].join(" "),
                        Some(expiry),
                    )),
                    Err(_) => ParseResult::InvalidInput("Invalid expiry time".to_string()),
                }
            } else {
                ParseResult::InvalidInput("Usage: WRITETTL <key> <seconds> <value>".to_string())
            }
        }
        "MWRITETTL" => {
            if words.len() < 4 || !words[2..].len().is_multiple_of(2) {
                ParseResult::InvalidInput(
                    "Usage: MWRITETTL <seconds> <key1> <value1> <keyn> <valuen>".to_string(),
                )
            } else {
                match words[1].parse::<u32>() {
                    Ok(0) => ParseResult::InvalidInput(
                        "Usage: MWRITETTL <seconds> <key1> <value1> <keyn> <valuen> \
                         (seconds must be >= 1)"
                            .to_string(),
                    ),
                    Ok(expiry) => {
                        let pairs: Vec<(String, String, Option<u32>)> = words[2..]
                            .chunks_exact(2)
                            .map(|chunk| (chunk[0].to_string(), chunk[1].to_string(), Some(expiry)))
                            .collect();
                        ParseResult::Cmd(Command::Mset(pairs))
                    }
                    Err(_) => ParseResult::InvalidInput("Invalid expiry time".to_string()),
                }
            }
        }
        _ => ParseResult::InvalidInput(format!("Unknown command: {}", words[0])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(line: &str) -> Command {
        match parse_command(line) {
            ParseResult::Cmd(c) => c,
            ParseResult::Quit => panic!("expected Cmd, got Quit"),
            ParseResult::InvalidInput(m) => panic!("expected Cmd, got InvalidInput: {m}"),
        }
    }

    fn is_invalid(line: &str) -> bool {
        matches!(parse_command(line), ParseResult::InvalidInput(_))
    }

    // --- WRITETTL ---

    #[test]
    fn writettl_happy_space_greedy_value() {
        match cmd("WRITETTL k 60 hello world") {
            Command::Write(key, value, ttl) => {
                assert_eq!(key, "k");
                assert_eq!(value, "hello world");
                assert_eq!(ttl, Some(60));
            }
            _ => panic!("expected Command::Write"),
        }
    }

    #[test]
    fn writettl_value_with_leading_digit() {
        match cmd("WRITETTL k 60 5things") {
            Command::Write(_, value, ttl) => {
                assert_eq!(value, "5things");
                assert_eq!(ttl, Some(60));
            }
            _ => panic!("expected Command::Write"),
        }
    }

    #[test]
    fn writettl_value_containing_ttl_and_ex_tokens() {
        match cmd("WRITETTL k 60 EX 5 TTL note") {
            Command::Write(_, value, ttl) => {
                assert_eq!(value, "EX 5 TTL note");
                assert_eq!(ttl, Some(60));
            }
            _ => panic!("expected Command::Write"),
        }
    }

    #[test]
    fn writettl_rejects_too_few_tokens() {
        assert!(is_invalid("WRITETTL k 60"));
    }

    #[test]
    fn writettl_rejects_non_numeric_seconds() {
        assert!(is_invalid("WRITETTL k abc value"));
    }

    #[test]
    fn writettl_rejects_zero_seconds() {
        assert!(is_invalid("WRITETTL k 0 value"));
    }

    // --- TTL ---

    #[test]
    fn ttl_happy() {
        match cmd("TTL k 30") {
            Command::Ttl(key, seconds) => {
                assert_eq!(key, "k");
                assert_eq!(seconds, 30);
            }
            _ => panic!("expected Command::Ttl"),
        }
    }

    #[test]
    fn ttl_zero_is_valid_persist() {
        match cmd("TTL k 0") {
            Command::Ttl(key, seconds) => {
                assert_eq!(key, "k");
                assert_eq!(seconds, 0);
            }
            _ => panic!("expected Command::Ttl"),
        }
    }

    #[test]
    fn ttl_rejects_wrong_arity() {
        assert!(is_invalid("TTL k"));
        assert!(is_invalid("TTL k 30 extra"));
    }

    #[test]
    fn ttl_rejects_non_numeric() {
        assert!(is_invalid("TTL k abc"));
    }

    // --- INCR ---

    #[test]
    fn incr_happy() {
        match cmd("INCR counter") {
            Command::Incr(key) => assert_eq!(key, "counter"),
            _ => panic!("expected Command::Incr"),
        }
    }

    #[test]
    fn incr_rejects_wrong_arity() {
        assert!(is_invalid("INCR"));
        assert!(is_invalid("INCR k extra"));
    }

    // --- MWRITETTL ---

    #[test]
    fn mwritettl_happy_uniform_ttl() {
        match cmd("MWRITETTL 60 k1 v1 k2 v2") {
            Command::Mset(pairs) => {
                assert_eq!(
                    pairs,
                    vec![
                        ("k1".to_string(), "v1".to_string(), Some(60)),
                        ("k2".to_string(), "v2".to_string(), Some(60)),
                    ]
                );
            }
            _ => panic!("expected Command::Mset"),
        }
    }

    #[test]
    fn mwritettl_rejects_zero_seconds() {
        assert!(is_invalid("MWRITETTL 0 k1 v1"));
    }

    #[test]
    fn mwritettl_rejects_odd_trailing_token() {
        assert!(is_invalid("MWRITETTL 60 k1 v1 k2"));
    }

    #[test]
    fn mwritettl_rejects_too_few_tokens() {
        assert!(is_invalid("MWRITETTL 60 k1"));
    }

    // --- Regression: non-TTL verbs carry None ---

    #[test]
    fn write_carries_none_ttl() {
        match cmd("WRITE k hello world") {
            Command::Write(key, value, ttl) => {
                assert_eq!(key, "k");
                assert_eq!(value, "hello world");
                assert_eq!(ttl, None);
            }
            _ => panic!("expected Command::Write"),
        }
    }

    #[test]
    fn mset_carries_none_ttl() {
        match cmd("MSET k1 v1 k2 v2") {
            Command::Mset(pairs) => {
                assert_eq!(
                    pairs,
                    vec![
                        ("k1".to_string(), "v1".to_string(), None),
                        ("k2".to_string(), "v2".to_string(), None),
                    ]
                );
            }
            _ => panic!("expected Command::Mset"),
        }
    }
}
