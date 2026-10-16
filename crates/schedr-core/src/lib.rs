//! Core scheduling engine for schedr.

/// Returns a greeting for `name`. Placeholder used to verify the toolchain.
#[must_use]
pub fn greet(name: &str) -> String {
    format!("Hello, {name}!")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greet_formats_name() {
        assert_eq!(greet("schedr"), "Hello, schedr!");
    }
}
