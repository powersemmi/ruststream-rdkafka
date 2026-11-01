//! A scratch directory for the unit tests, removed when dropped.

use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;

pub(crate) struct TestDir(TempDir);

impl TestDir {
    pub(crate) fn new() -> Self {
        Self(TempDir::new().expect("temp dir"))
    }

    pub(crate) fn path(&self, relative: &str) -> PathBuf {
        self.0.path().join(relative)
    }

    pub(crate) fn write(&self, relative: &str, text: &str) {
        let path = self.path(relative);
        fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
        fs::write(path, text).expect("write");
    }

    pub(crate) fn read(&self, relative: &str) -> String {
        fs::read_to_string(self.path(relative)).expect("read")
    }
}
