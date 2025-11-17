#![deny(unused_must_use)]

mod backend;
use std::{io::Write, path::Path};

pub use backend::*;
use sha1::{Digest, Sha1};

mod backend_filesystem;
mod backend_handler;

mod account;
mod arcfactory;
mod directories;
mod install_content;
mod instance;
mod launch;
mod launch_wrapper;
mod log_reader;
mod metadata;
mod mod_metadata;

pub(crate) fn is_single_component_path(path: &str) -> bool {
    let path = std::path::Path::new(path);
    let mut components = path.components().peekable();

    if let Some(first) = components.peek() && !matches!(first, std::path::Component::Normal(_)) {
        return false;
    }

    components.count() == 1
}

pub(crate) fn check_sha1_hash(path: &Path, expected_hash: [u8; 20]) -> std::io::Result<bool> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha1::new();
    let _ = std::io::copy(&mut file, &mut hasher)?;

    let actual_hash = hasher.finalize();

    Ok(expected_hash == *actual_hash)
}

pub(crate) fn write_safe(path: impl AsRef<Path>, content: impl AsRef<[u8]>) -> std::io::Result<()> {
    let path = path.as_ref();
    let content = content.as_ref();

    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let mut temp = path.to_path_buf();
    temp.add_extension("new");

    let mut temp_file = std::fs::File::create(&temp)?;

    temp_file.write_all(content)?;
    temp_file.flush()?;
    temp_file.sync_all()?;

    drop(temp_file);

    std::fs::rename(temp, path)?;

    Ok(())
}
