//! Probe 1: Linux kernel version ≥ 5.13.
//!
//! The Landlock LSM is available from kernel 5.13 onwards, and the
//! unprivileged-overlayfs + `xino=on` features cairn relies on landed in
//! 5.11 and 5.9 respectively. 5.13 is therefore the effective floor.
//!
//! We read `/proc/sys/kernel/osrelease` (always present on Linux, always
//! readable unprivileged), then parse `MAJOR.MINOR` from the prefix.

use std::fs;

use crate::ProbeResult;

const NUMBER: u8 = 1;
const NAME: &str = "Linux kernel >= 5.13";
const REQUIRED: bool = true;
const MIN_MAJOR: u32 = 5;
const MIN_MINOR: u32 = 13;

pub fn probe() -> ProbeResult {
    let release = match fs::read_to_string("/proc/sys/kernel/osrelease") {
        Ok(s) => s.trim().to_string(),
        Err(err) => {
            return ProbeResult::fail(
                NUMBER,
                NAME,
                REQUIRED,
                Some(format!("{err}")),
                format!(
                    "/proc/sys/kernel/osrelease unreadable: {err}. This probe requires a Linux host."
                ),
            );
        }
    };

    match parse_major_minor(&release) {
        Ok((major, minor)) => {
            let meets = (major, minor) >= (MIN_MAJOR, MIN_MINOR);
            if meets {
                ProbeResult::pass(
                    NUMBER,
                    NAME,
                    REQUIRED,
                    format!("{release} — {major}.{minor} ≥ {MIN_MAJOR}.{MIN_MINOR}"),
                )
            } else {
                ProbeResult::fail(
                    NUMBER,
                    NAME,
                    REQUIRED,
                    None,
                    format!(
                        "{release} — kernel {major}.{minor} is below the {MIN_MAJOR}.{MIN_MINOR} floor. \
                         Landlock (5.13+) is required; upgrade the kernel before running cairn's sandbox."
                    ),
                )
            }
        }
        Err(err) => ProbeResult::fail(
            NUMBER,
            NAME,
            REQUIRED,
            None,
            format!("unparseable osrelease `{release}`: {err}"),
        ),
    }
}

/// Parse `MAJOR.MINOR` from a string like `6.17.0-1010-aws`. Only the first
/// two dot-separated integer components are needed.
pub fn parse_major_minor(release: &str) -> Result<(u32, u32), String> {
    let mut parts = release.split('.');
    let major: u32 = parts
        .next()
        .ok_or_else(|| "missing major".to_string())?
        .parse()
        .map_err(|e: std::num::ParseIntError| format!("major parse: {e}"))?;
    let minor_part = parts.next().ok_or_else(|| "missing minor".to_string())?;
    // The minor segment can contain trailing characters (e.g. `17+`); strip
    // everything after the leading digit run before parsing.
    let minor_digits: String = minor_part
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if minor_digits.is_empty() {
        return Err("minor has no leading digits".to_string());
    }
    let minor: u32 = minor_digits
        .parse()
        .map_err(|e: std::num::ParseIntError| format!("minor parse: {e}"))?;
    Ok((major, minor))
}

#[cfg(test)]
mod tests {
    use super::parse_major_minor;

    #[test]
    fn parses_aws_style_release() {
        assert_eq!(parse_major_minor("6.17.0-1010-aws").unwrap(), (6, 17));
    }

    #[test]
    fn parses_plain_major_minor() {
        assert_eq!(parse_major_minor("5.13").unwrap(), (5, 13));
    }

    #[test]
    fn parses_release_with_extras_on_minor() {
        assert_eq!(parse_major_minor("5.15+custom").unwrap(), (5, 15));
    }

    #[test]
    fn rejects_missing_minor() {
        assert!(parse_major_minor("6").is_err());
    }

    #[test]
    fn rejects_non_numeric_major() {
        assert!(parse_major_minor("six.17").is_err());
    }
}
