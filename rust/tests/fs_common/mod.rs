use std::fs;
use std::path::Path;

pub fn cleanup_dir_except<P: AsRef<Path>>(path: P, ignore_files: Vec<String>) {
    for p in fs::read_dir(path).unwrap() {
        if let Ok(d) = p {
            let path = d.path();
            let name = d.path().file_name().unwrap().to_str().unwrap().to_string();

            if !ignore_files.contains(&name) && !name.starts_with(".") {
                fs::remove_file(&path).unwrap();
            }
        }
    }
}
