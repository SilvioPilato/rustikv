use rustikv::utils::prefix_successor;

#[test]
fn ascii_increments_last_char() {
    assert_eq!(prefix_successor("cpu").as_deref(), Some("cpv"));
}

#[test]
fn successor_is_strictly_above_all_matching_keys() {
    let succ = prefix_successor("cpu").unwrap();
    assert!("cpu".to_string() < succ);
    assert!("cpu:host1".to_string() < succ);
    assert!("cpu\u{10FFFF}".to_string() < succ);
    assert!("cpv".to_string() >= succ);
}

#[test]
fn empty_prefix_has_no_successor() {
    assert_eq!(prefix_successor(""), None);
}

#[test]
fn trailing_char_max_carries_to_previous_scalar() {
    assert_eq!(prefix_successor("a\u{10FFFF}").as_deref(), Some("b"));
}

#[test]
fn all_char_max_has_no_successor() {
    assert_eq!(prefix_successor("\u{10FFFF}\u{10FFFF}"), None);
}

#[test]
fn skips_surrogate_gap() {
    assert_eq!(prefix_successor("\u{D7FF}").as_deref(), Some("\u{E000}"));
}

#[test]
fn multibyte_prefix_increments_last_scalar() {
    assert_eq!(prefix_successor("é").as_deref(), Some("\u{00EA}"));
}
