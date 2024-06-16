pub fn escape_word(mut word: &str) -> String {
    if word.starts_with('\'') && word.ends_with('\'') {
        word = &word[1..word.len() - 1];
    }

    let mut rs = String::new();
    let mut pos = 0;
    while pos < word.len() {
        pos += match word[pos..].find('\\') {
            Some(offset) => {
                let ch = &word[pos + offset + 1..pos + offset + 2];
                let ch = match ch {
                    "n" => "\n",
                    _ => ch,
                };
                rs = format!("{rs}{}{ch}", &word[pos..pos + offset]);
                offset + 2
            }
            None => {
                rs = format!("{rs}{}", &word[pos..]);
                break;
            }
        };
    }
    rs
}

#[test]
fn test() {
    let rs = escape_word("\\wo\\nrd");
    assert_eq!(rs, "wo\nrd");
}
