//! What `--help` says, and what it keeps to itself.

use crate::support::Crate;

#[test]
fn help_names_every_command_and_never_prints_a_credential() {
    let krate = Crate::new();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-kafka-schema"));
    let output = command
        .current_dir(krate.root())
        .args(["kafka-schema", "--help"])
        .env(
            "RUSTSTREAM_SCHEMA_REGISTRY",
            "https://svc:hunter2@registry:8081",
        )
        .env("RUSTSTREAM_SCHEMA_REGISTRY_PASSWORD", "hunter3")
        .output()
        .expect("help");
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{help}");
    for command in ["migrate", "prepare", "check"] {
        assert!(help.contains(command), "{help}");
    }
    assert!(help.contains("RUSTSTREAM_SCHEMA_REGISTRY"), "{help}");
    assert!(
        !help.contains("hunter2") && !help.contains("hunter3"),
        "{help}"
    );

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-kafka-schema"))
        .args(["kafka-schema", "migrate", "--help"])
        .output()
        .expect("migrate help");
    let help = String::from_utf8_lossy(&output.stdout);
    for command in ["add", "import", "status", "run"] {
        assert!(help.contains(command), "{help}");
    }
}

#[test]
fn a_failing_registry_url_is_never_echoed() {
    let krate = Crate::new().online("://svc:hunter2@registry");
    let stderr = krate.tool(&["migrate", "status"]).fails();
    assert!(
        stderr.contains("the registry URL does not parse"),
        "{stderr}"
    );
    assert!(!stderr.contains("hunter2"), "{stderr}");
}
