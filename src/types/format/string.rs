/// Parse a PostgreSQL string literal
pub fn parse_string_postgres(literal: &str, standard_conforming_strings: bool) -> String {
    if standard_conforming_strings {
        // Standard mode: only '' is an escape sequence
        literal.replace("''", "'")
    } else {
        // Legacy mode: process backslash escapes
        parse_legacy_string(literal)
    }
}

/// Parse a string with legacy backslash escapes
fn parse_legacy_string(content: &str) -> String {
    let mut result = String::new();
    let mut chars = content.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('\'') => result.push('\''),
                Some('\\') => result.push('\\'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('0') => result.push('\0'),
                Some(c) => result.push(c), // Unknown escape, treat as literal
                None => {}
            }
        } else {
            result.push(ch);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_string() {
        let value = "ab''c";
        assert_eq!("ab'c", parse_string_postgres(value, true));
    }
}
