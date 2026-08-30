//! Warn when a client flag overrides its environment variable.

/// Warn when a flag overrides a differing value of its environment variable.
/// clap prefers the flag silently; a parsed value that differs from a set
/// variable can only have come from the flag. Values are compared after
/// parsing, so `1G` and `1024M` agree. `show` controls whether the values are
/// printed (not for secrets).
pub fn warn_if_overridden<T: PartialEq + std::fmt::Display>(
    flag: &str,
    var: &str,
    value: Option<&T>,
    parse: impl Fn(&str) -> Option<T>,
    show: bool,
) {
    let (Ok(env), Some(value)) = (std::env::var(var), value) else {
        return;
    };
    if parse(&env).as_ref() != Some(value) {
        if show {
            eprintln!("warning: --{flag}={value} overrides {var}={env}");
        } else {
            eprintln!("warning: --{flag} overrides a different {var}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_var<R>(var: &str, value: Option<&str>, f: impl FnOnce() -> R) -> R {
        // SAFETY: each test uses its own variable name and nothing else in
        // this process reads it.
        unsafe {
            match value {
                Some(v) => std::env::set_var(var, v),
                None => std::env::remove_var(var),
            }
        }
        let r = f();
        // SAFETY: as above.
        unsafe { std::env::remove_var(var) };
        r
    }

    fn parse(s: &str) -> Option<u32> {
        s.parse().ok()
    }

    #[test]
    fn silent_without_variable_or_value() {
        with_var("OXICACHE_TEST_A", None, || {
            warn_if_overridden("a", "OXICACHE_TEST_A", Some(&1u32), parse, true);
        });
        with_var("OXICACHE_TEST_B", Some("1"), || {
            warn_if_overridden("b", "OXICACHE_TEST_B", None::<&u32>, parse, true);
        });
    }

    #[test]
    fn silent_when_flag_agrees_with_variable() {
        with_var("OXICACHE_TEST_C", Some("1"), || {
            warn_if_overridden("c", "OXICACHE_TEST_C", Some(&1u32), parse, true);
        });
    }

    #[test]
    fn warns_when_flag_differs() {
        with_var("OXICACHE_TEST_D", Some("1"), || {
            warn_if_overridden("d", "OXICACHE_TEST_D", Some(&2u32), parse, true);
            warn_if_overridden("d", "OXICACHE_TEST_D", Some(&2u32), parse, false);
        });
    }
}
