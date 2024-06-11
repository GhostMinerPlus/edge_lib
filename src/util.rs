pub fn escape_word(word: &str) -> String {
    let mut word = word.replace("\\'", "'");
    if word.starts_with('\'') && word.ends_with('\'') {
        word = word[1..word.len() - 1].to_string();
    }
    word
}
