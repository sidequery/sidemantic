//! Relative date parsing for filter expressions
//!
//! Parses expressions like "last 7 days", "this month", "yesterday" into SQL.

use lazy_static::lazy_static;
use regex::Regex;

/// Relative date expression parser
pub struct RelativeDate;

lazy_static! {
    static ref LAST_N_DAYS: Regex = Regex::new(r"^last (\d+) days?$").unwrap();
    static ref LAST_N_WEEKS: Regex = Regex::new(r"^last (\d+) weeks?$").unwrap();
    static ref LAST_N_MONTHS: Regex = Regex::new(r"^last (\d+) months?$").unwrap();
    static ref LAST_N_YEARS: Regex = Regex::new(r"^last (\d+) years?$").unwrap();
}

impl RelativeDate {
    /// Parse a relative date expression to SQL.
    ///
    /// Returns None if the expression is not recognized.
    ///
    /// # Examples
    /// ```
    /// use sidemantic::RelativeDate;
    /// assert_eq!(RelativeDate::parse("today"), Some("CURRENT_DATE".to_string()));
    /// assert_eq!(RelativeDate::parse("last 7 days"), Some("CURRENT_DATE - 7".to_string()));
    /// ```
    pub fn parse(expr: &str) -> Option<String> {
        let expr = expr.to_lowercase();
        let expr = expr.trim();

        // Simple keywords
        match expr {
            "today" => return Some("CURRENT_DATE".to_string()),
            "yesterday" => return Some("CURRENT_DATE - 1".to_string()),
            "tomorrow" => return Some("CURRENT_DATE + 1".to_string()),
            "this week" => return Some("DATE_TRUNC('week', CURRENT_DATE)".to_string()),
            "last week" => {
                return Some("DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'".to_string())
            }
            "next week" => {
                return Some("DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'".to_string())
            }
            "this month" => return Some("DATE_TRUNC('month', CURRENT_DATE)".to_string()),
            "last month" => {
                return Some("DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'".to_string())
            }
            "next month" => {
                return Some("DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'".to_string())
            }
            "this quarter" => return Some("DATE_TRUNC('quarter', CURRENT_DATE)".to_string()),
            "last quarter" => {
                return Some(
                    "DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'".to_string(),
                )
            }
            "next quarter" => {
                return Some(
                    "DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'".to_string(),
                )
            }
            "this year" => return Some("DATE_TRUNC('year', CURRENT_DATE)".to_string()),
            "last year" => {
                return Some("DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'".to_string())
            }
            "next year" => {
                return Some("DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'".to_string())
            }
            _ => {}
        }

        // Last N days/weeks/months/years
        if let Some(caps) = LAST_N_DAYS.captures(expr) {
            let n: i32 = caps[1].parse().ok()?;
            return Some(format!("CURRENT_DATE - {n}"));
        }

        if let Some(caps) = LAST_N_WEEKS.captures(expr) {
            let n: i32 = caps[1].parse().ok()?;
            return Some(format!("CURRENT_DATE - {}", n * 7));
        }

        if let Some(caps) = LAST_N_MONTHS.captures(expr) {
            let n: i32 = caps[1].parse().ok()?;
            return Some(format!(
                "DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{n} months'"
            ));
        }

        if let Some(caps) = LAST_N_YEARS.captures(expr) {
            let n: i32 = caps[1].parse().ok()?;
            return Some(format!(
                "DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '{n} years'"
            ));
        }

        None
    }

    /// Convert a relative date expression to a SQL range filter.
    ///
    /// # Examples
    /// ```
    /// use sidemantic::RelativeDate;
    /// assert_eq!(
    ///     RelativeDate::to_range("last 7 days", "created_at"),
    ///     Some("created_at >= CURRENT_DATE - 7".to_string())
    /// );
    /// ```
    pub fn to_range(expr: &str, column: &str) -> Option<String> {
        let expr_lower = expr.to_lowercase();
        let expr_lower = expr_lower.trim();

        // For single day expressions - exact match
        match expr_lower {
            "today" | "yesterday" | "tomorrow" => {
                let sql = Self::parse(expr)?;
                return Some(format!("{column} = {sql}"));
            }
            _ => {}
        }

        // For "last N days/weeks" - use >= comparison
        if expr_lower.starts_with("last ")
            && (expr_lower.contains("day") || expr_lower.contains("week"))
        {
            let sql = Self::parse(expr)?;
            return Some(format!("{column} >= {sql}"));
        }

        // For "this/last/next month/quarter/year" - use range
        if (expr_lower.contains("month")
            || expr_lower.contains("quarter")
            || expr_lower.contains("year"))
            && (expr_lower.starts_with("this ")
                || expr_lower.starts_with("last ")
                || expr_lower.starts_with("next "))
        {
            let start_sql = Self::parse(expr)?;
            let interval = if expr_lower.contains("month") {
                "1 month"
            } else if expr_lower.contains("quarter") {
                "3 months"
            } else {
                "1 year"
            };
            return Some(format!(
                "{column} >= {start_sql} AND {column} < {start_sql} + INTERVAL '{interval}'"
            ));
        }

        // For "this/last/next week" - use range
        if expr_lower.contains("week")
            && (expr_lower.starts_with("this ")
                || expr_lower.starts_with("last ")
                || expr_lower.starts_with("next "))
        {
            let start_sql = Self::parse(expr)?;
            return Some(format!(
                "{column} >= {start_sql} AND {column} < {start_sql} + INTERVAL '1 week'"
            ));
        }

        None
    }

    /// Check if an expression is a recognized relative date.
    pub fn is_relative_date(expr: &str) -> bool {
        Self::parse(expr).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        assert_eq!(RelativeDate::parse("today"), Some("CURRENT_DATE".into()));
        assert_eq!(
            RelativeDate::parse("yesterday"),
            Some("CURRENT_DATE - 1".into())
        );
        assert_eq!(
            RelativeDate::parse("this month"),
            Some("DATE_TRUNC('month', CURRENT_DATE)".into())
        );
    }

    #[test]
    fn test_parse_last_n() {
        assert_eq!(
            RelativeDate::parse("last 7 days"),
            Some("CURRENT_DATE - 7".into())
        );
        assert_eq!(
            RelativeDate::parse("last 30 days"),
            Some("CURRENT_DATE - 30".into())
        );
        assert_eq!(
            RelativeDate::parse("last 2 weeks"),
            Some("CURRENT_DATE - 14".into())
        );
    }

    #[test]
    fn test_to_range() {
        assert_eq!(
            RelativeDate::to_range("last 7 days", "created_at"),
            Some("created_at >= CURRENT_DATE - 7".into())
        );
        assert!(RelativeDate::to_range("this month", "order_date")
            .unwrap()
            .contains("INTERVAL '1 month'"));
    }

    #[test]
    fn test_is_relative_date() {
        assert!(RelativeDate::is_relative_date("today"));
        assert!(RelativeDate::is_relative_date("last 7 days"));
        assert!(!RelativeDate::is_relative_date("invalid"));
    }
}
