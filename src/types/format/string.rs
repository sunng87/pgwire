/// Parse a PostgreSQL string literal
pub fn parse_string_postgres(literal: &str) -> String {
    // Standard mode: only '' is an escape sequence
    literal.replace("''", "'")
}

#[cfg(test)]
mod tests {
    use super::parse_string_postgres;

    #[test]
    fn test_parse_string() {
        let value = "ab''c";
        assert_eq!("ab'c", parse_string_postgres(value));
    }
}
