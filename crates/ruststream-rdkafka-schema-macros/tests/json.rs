//! The JSON Schema check, compiled the way a service compiles it: each case under `tests/json/` is
//! a crate of its own, checked against the subjects under `tests/json/schemas/`.

use std::env;
use std::path::PathBuf;
use std::process::Command;

/// Where the derive looks for the migrations.
const SCHEMA_DIR: &str = "RUSTSTREAM_SCHEMA_DIR";

#[test]
fn json_check() {
    let schemas = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/json/schemas");
    // Each case compiles outside this crate, so the derive finds the subjects through the
    // environment. The test hands the variable to a run of itself instead of setting it in its
    // own process.
    if env::var_os(SCHEMA_DIR).as_deref() != Some(schemas.as_os_str()) {
        let status = Command::new(env::current_exe().expect("the test binary has a path"))
            .args(["json_check", "--exact", "--nocapture"])
            .env(SCHEMA_DIR, &schemas)
            .status()
            .expect("the test binary runs again");
        assert!(status.success(), "the JSON check cases failed");
        return;
    }
    let cases = trybuild::TestCases::new();
    cases.pass("tests/json/pass/*.rs");
    cases.compile_fail("tests/json/fail/*.rs");
}
