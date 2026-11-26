use std::borrow::Cow;

/// Remove quotes from quoted identifier
pub fn unquote<'a>(quoted: &'a str) -> Cow<'a, str> {
    if !quoted.starts_with('"') || !quoted.ends_with('"') {
        // Not a properly quoted identifier, return as-is
        return Cow::Borrowed(quoted);
    }

    // Remove the outer quotes
    let inner = &quoted[1..quoted.len() - 1];

    let mut result = String::with_capacity(inner.len());
    let mut chars = inner.char_indices().peekable();

    while let Some((_, c)) = chars.next() {
        if c == '"' {
            // Look ahead to see if this is an escaped quote
            if let Some(&(_, next_c)) = chars.peek() {
                if next_c == '"' {
                    // This is an escaped quote - add one " and skip the next
                    result.push('"');
                    chars.next(); // Skip the second quote
                } else {
                    // This shouldn't happen in valid PostgreSQL identifiers
                    // (unescaped quote inside identifier), but we'll preserve it
                    result.push('"');
                }
            } else {
                // Quote at the end (shouldn't happen after removing outer quotes)
                result.push('"');
            }
        } else {
            result.push(c);
        }
    }

    Cow::Owned(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_unquoting() {
        assert_eq!(unquote(r#""mytable""#), "mytable");
        assert_eq!(unquote(r#""MyTable""#), "MyTable");
        assert_eq!(unquote(r#""my table""#), "my table");
        assert_eq!(unquote(r#""123_table""#), "123_table");
        assert_eq!(unquote(r#""_weird@#$""#), "_weird@#$");
    }

    #[test]
    fn test_escaped_quotes() {
        assert_eq!(unquote(r#""""hello""""#), r#""hello""#);
        assert_eq!(unquote(r#""quote""inside""#), r#"quote"inside"#);
        assert_eq!(unquote(r#""start""end""#), r#"start"end"#);
        assert_eq!(unquote(r#""""prefix""#), r#""prefix"#);
        assert_eq!(unquote(r#""suffix""""#), r#"suffix""#);
        assert_eq!(unquote(r#""multiple""""quotes""#), r#"multiple""quotes"#);
    }

    #[test]
    fn test_multiple_escaped_quotes() {
        assert_eq!(unquote(r#""three""""""quotes""#), r#"three"""quotes"#);
        assert_eq!(unquote(r#""mixed""and regular""#), r#"mixed"and regular"#);
        assert_eq!(unquote(r#""start""mid""end""#), r#"start"mid"end"#);
        assert_eq!(unquote(r#""quote""at""end""""#), r#"quote"at"end""#);
    }

    #[test]
    fn test_edge_cases() {
        assert_eq!(unquote(r#""""#), ""); // Empty identifier
        assert_eq!(unquote(r#""""""#), r#"""#); // One double quote
        assert_eq!(unquote(r#"" """" ""#), r#" "" "#); // Mixed spaces and quotes
    }

    #[test]
    fn test_invalid_input() {
        // No outer quotes - return as-is
        assert_eq!(unquote("mytable"), "mytable");
        assert_eq!(unquote("hello\"world"), "hello\"world");

        // Missing closing quote - treat as not properly quoted
        assert_eq!(unquote(r#""incomplete"#), r#""incomplete"#);

        // Missing opening quote - treat as not properly quoted
        assert_eq!(unquote(r#"incomplete""#), r#"incomplete""#);
    }
}
