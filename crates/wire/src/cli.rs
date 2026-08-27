//! Helpers shared by the server and client command lines.

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
