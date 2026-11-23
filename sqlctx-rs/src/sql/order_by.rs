//! Preserve complete result names when splitting ordering suffixes.

fn suffix(source: &str) -> Option<String> {
    let value = source
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    matches!(
        value.as_str(),
        "" | "ASC"
            | "DESC"
            | "NULLS FIRST"
            | "NULLS LAST"
            | "ASC NULLS FIRST"
            | "ASC NULLS LAST"
            | "DESC NULLS FIRST"
            | "DESC NULLS LAST"
    )
    .then_some(value)
}

pub(crate) fn split_order_field<'a>(item: &'a str, known_fields: &[&str]) -> (&'a str, String) {
    let item = item.trim();
    let known = known_fields
        .iter()
        .filter_map(|field| {
            let remainder = item.strip_prefix(*field)?;
            if !remainder.is_empty() && !remainder.starts_with(char::is_whitespace) {
                return None;
            }
            suffix(remainder).map(|suffix| (field.len(), suffix))
        })
        .max_by_key(|(length, _)| *length);
    if let Some((length, suffix)) = known {
        return (&item[..length], suffix);
    }
    for (boundary, character) in item.char_indices() {
        if !character.is_whitespace() {
            continue;
        }
        if let Some(suffix) = suffix(&item[boundary..]) {
            return (&item[..boundary], suffix);
        }
    }
    (item, String::new())
}
