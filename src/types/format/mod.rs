use std::collections::HashMap;

pub mod bytea_output;
pub mod date_style;
pub mod float_digits;
pub mod interval_style;
pub mod string;

/// All possible format options of postgres
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
#[non_exhaustive]
pub struct FormatOptions {
    pub date_style: String,
    pub interval_style: String,
    pub bytea_output: String,
    pub time_zone: String,
    pub extra_float_digits: i8,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            date_style: "ISO, YMD".to_string(),
            interval_style: "postgres".to_string(),
            bytea_output: "hex".to_string(),
            time_zone: "Etc/UTC".to_string(),
            extra_float_digits: 1,
        }
    }
}

impl FormatOptions {
    /// Merge default format options with session level custom values
    pub fn from_client_metadata(metadata_map: &HashMap<String, String>) -> Self {
        let mut format_options = Self::default();

        // check metadata map for each field
        if let Some(value) = metadata_map.get("datestyle") {
            format_options.date_style = value.to_string();
        }

        if let Some(value) = metadata_map.get("intervalstyle") {
            format_options.interval_style = value.to_string();
        }

        if let Some(value) = metadata_map.get("bytea_output") {
            format_options.bytea_output = value.to_string();
        }

        if let Some(value) = metadata_map.get("extra_float_digits") {
            if let Ok(value) = value.parse::<i8>() {
                format_options.extra_float_digits = value;
            }
        }

        format_options
    }
}
