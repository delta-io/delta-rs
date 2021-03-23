#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod platform_cfg {
    // glibc version is taken from std/sys/unix/os.rs
    pub fn glibc_version() -> (usize, usize) {
        use regex::Regex;
        use std::process::Command;

        let output = Command::new("ldd")
            .args(&["--version"])
            .output()
            .expect("failed to execute ldd");
        let output_str = std::str::from_utf8(&output.stdout).unwrap();

        let version_reg = Regex::new(r#"ldd \(.+\) ([0-9]+\.[0-9]+)"#).unwrap();
        if let Some(captures) = version_reg.captures(output_str) {
            let version_str = captures.get(1).unwrap().as_str();
            parse_glibc_version(version_str).unwrap()
        } else {
            panic!(
                "ERROR: failed to detect glibc version. ldd output: {}",
                output_str
            );
        }
    }

    // Returns Some((major, minor)) if the string is a valid "x.y" version,
    // ignoring any extra dot-separated parts. Otherwise return None.
    fn parse_glibc_version(version: &str) -> Option<(usize, usize)> {
        let mut parsed_ints = version.split('.').map(str::parse::<usize>).fuse();
        match (parsed_ints.next(), parsed_ints.next()) {
            (Some(Ok(major)), Some(Ok(minor))) => Some((major, minor)),
            _ => None,
        }
    }

    fn detect_glibc_renameat2() {
        let (major, minor) = glibc_version();
        if major >= 2 && minor >= 28 {
            println!("cargo:rustc-cfg=glibc_renameat2");
        }
    }

    pub fn set() {
        detect_glibc_renameat2();
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
mod platform_cfg {
    pub fn set() {}
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    platform_cfg::set();
}
