#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod platform_cfg {
    fn detect_glibc_renameat2() {
        // we should never fail on version parsing when the target is linux + glibc
        let ver = glibc_version::get_version().unwrap();
        if ver.major >= 2 && ver.minor >= 28 {
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
