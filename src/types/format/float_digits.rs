use crate::error::PgWireError;

/// extra_float_digits sets the number of digits displayed for floating-point
/// values.
///
/// Valid values [-15, 3], default value: 1
#[derive(Debug)]
pub struct ExtraFloatDigits(pub i8);

impl TryFrom<&str> for ExtraFloatDigits {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = value
            .parse::<i8>()
            .map_err(|_| PgWireError::InvalidOptionValue(value.to_string()))?;

        if !(-15..=3).contains(&value) {
            Err(PgWireError::InvalidOptionValue(value.to_string()))
        } else {
            Ok(Self(value))
        }
    }
}

impl ExtraFloatDigits {
    const FLT_DIG: i8 = 6;
    const DBL_DIG: i8 = 15;

    pub fn format_f64(&self, value: f64) -> String {
        let extra = self.0.clamp(-15, 3);

        // Handle special cases
        if value.is_nan() {
            return "NaN".to_string();
        }
        if value.is_infinite() {
            return if value.is_sign_positive() {
                "inf".to_string()
            } else {
                "-inf".to_string()
            };
        }

        if value == 0.0 {
            return "0".to_string();
        }

        // For extra_float_digits >= 1, use ryu for shortest-precise format
        // This preserves the original binary float value exactly
        // At most 17 digits for f64
        if extra >= 1 {
            let mut buffer = ryu::Buffer::new();
            return buffer.format(value).to_string();
        }

        // For negative values, round to DBL_DIG + extra_float_digits significant digits
        // DBL_DIG = 15 for f64
        let sig_digits = (Self::DBL_DIG + extra).max(0) as usize;
        format_with_significant_digits(value, sig_digits)
    }

    pub fn format_f32(&self, value: f32) -> String {
        let extra = self.0.clamp(-15, 3);

        // Handle special cases
        if value.is_nan() {
            return "NaN".to_string();
        }
        if value.is_infinite() {
            return if value.is_sign_positive() {
                "inf".to_string()
            } else {
                "-inf".to_string()
            };
        }

        if value == 0.0 {
            return "0".to_string();
        }

        // For extra_float_digits >= 1, use ryu for shortest-precise format
        // This preserves the original binary float value exactly
        // At most 9 digits for f32
        if extra >= 1 {
            let mut buffer = ryu::Buffer::new();
            return buffer.format(value).to_string();
        }

        // For negative values, round to FLT_DIG + extra_float_digits significant digits
        // FLT_DIG = 6 for f32
        let sig_digits = (Self::FLT_DIG + extra).max(0) as usize;
        format_with_significant_digits(value as f64, sig_digits)
    }
}

/// Format with specific number of significant digits
fn format_with_significant_digits(value: f64, sig_digits: usize) -> String {
    if sig_digits == 0 {
        // Round to nearest integer
        return format!("{:.0}", value.round());
    }

    let negative = value < 0.0;
    let abs_value = value.abs();

    // Find the order of magnitude
    let magnitude = abs_value.log10().floor();

    // Calculate decimal places needed
    // For a number like 3.14159, magnitude is 0
    // For 314.159, magnitude is 2
    // For 0.0314159, magnitude is -2
    let decimal_places = if magnitude >= 0.0 {
        // For numbers >= 1, decimal places = sig_digits - (integer_digits)
        let integer_digits = (magnitude as i32) + 1;
        (sig_digits as i32 - integer_digits).max(0) as usize
    } else {
        // For numbers < 1, we need to account for leading zeros after decimal
        let leading_zeros = (-magnitude as i32) - 1;
        sig_digits + leading_zeros as usize
    };

    // Format with calculated precision
    let formatted = format!("{abs_value:.decimal_places$}");

    // Remove trailing zeros and unnecessary decimal point
    let trimmed = trim_trailing_zeros(&formatted);

    if negative {
        format!("-{trimmed}",)
    } else {
        trimmed
    }
}

/// Remove trailing zeros from decimal representation
fn trim_trailing_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }

    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    trimmed.to_string()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format() {
        let v = std::f64::consts::PI; //3.1415926535897931

        let formatted = &[
            "3",                 // -15
            "3",                 // -14
            "3.1",               // -13
            "3.14",              // -12
            "3.142",             // -11
            "3.1416",            // -10
            "3.14159",           // -9
            "3.141593",          // -8
            "3.1415927",         // -7
            "3.14159265",        // -6
            "3.141592654",       // -5
            "3.1415926536",      // -4
            "3.14159265359",     // -3
            "3.14159265359",     // -2
            "3.1415926535898",   // -1
            "3.14159265358979",  // 0
            "3.141592653589793", // 1
            "3.141592653589793", // 2
            "3.141592653589793", // 3
        ];

        for (i, e) in (-15..=3).enumerate() {
            let extra_float_digit = ExtraFloatDigits(e);
            assert_eq!(extra_float_digit.format_f64(v), formatted[i]);
        }

        assert_eq!(ExtraFloatDigits(0).format_f64(f64::NAN), "NaN");
        assert_eq!(ExtraFloatDigits(0).format_f64(f64::INFINITY), "inf");
        assert_eq!(ExtraFloatDigits(0).format_f64(-f64::INFINITY), "-inf");

        let v = std::f32::consts::PI; //3.1415926535897931

        let formatted = &[
            "3",         // -15
            "3",         // -14
            "3",         // -13
            "3",         // -12
            "3",         // -11
            "3",         // -10
            "3",         // -9
            "3",         // -8
            "3",         // -7
            "3",         // -6
            "3",         // -5
            "3.1",       // -4
            "3.14",      // -3
            "3.142",     // -2
            "3.1416",    // -1
            "3.14159",   // 0
            "3.1415927", // 1
            "3.1415927", // 2
            "3.1415927", // 3
        ];

        for (i, e) in (-15..=3).enumerate() {
            let extra_float_digit = ExtraFloatDigits(e);
            assert_eq!(extra_float_digit.format_f32(v), formatted[i]);
        }
    }
}
