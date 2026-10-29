//! The derive's checks as a service meets them: each case under `tests/ui/` is a crate of its
//! own, compiled against the migrations in `tests/schemas/`. A failing case's message is pinned
//! next to it.

use std::env;
use std::path::Path;
use std::process::Command;

/// The variable that points the derive at the fixture migrations.
const SCHEMA_DIR: &str = "RUSTSTREAM_SCHEMA_DIR";

#[test]
fn ui() {
    // A compiler message changes between releases, so the cases run where the snapshots were
    // written: the stable toolchain, under `RUN_UI_TESTS=1`.
    if env::var_os("RUN_UI_TESTS").is_none() {
        return;
    }
    // The cases build outside the source tree, where the derive finds no `schemas/` of its
    // own. The variable reaches their build through this test's environment, so the test runs
    // itself again with it set rather than changing its own process's environment.
    if env::var_os(SCHEMA_DIR).is_none() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/schemas");
        let status = Command::new(env::current_exe().expect("the test binary has a path"))
            .args(["ui", "--exact", "--nocapture", "--test-threads=1"])
            .env(SCHEMA_DIR, fixtures)
            .status()
            .expect("the test binary runs");
        assert!(status.success(), "the compile cases failed");
        return;
    }
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/pass/*.rs");
    cases.compile_fail("tests/ui/fail/*.rs");
}
