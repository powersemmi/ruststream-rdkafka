//! `check`, `prepare` and `migrate status`, offline: the CI gate over the directory, the lock and
//! the exported schemas.

use crate::support::Crate;

const ORDER_V1: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"}]}"#;
const ORDER_V2: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"note","type":["null","string"],"default":null}]}"#;
const PAYMENT: &str = "syntax = \"proto3\";\npackage acme;\nmessage Payment { int64 id = 1; }\n";

fn orders() -> Crate {
    let krate = Crate::new();
    krate.exporting(&[("com.acme.Order", "avsc", ORDER_V1)]);
    krate
        .tool(&[
            "migrate",
            "add",
            "orders-value",
            "create_order",
            "--record",
            "com.acme.Order",
        ])
        .ok();
    krate
}

/// Marks the subject's one migration as registered, as `migrate run` would.
fn mark_applied(krate: &Crate) {
    let lock = krate.read("schemas/kafka-schema.lock");
    let applied: String = lock
        .lines()
        .map(|line| {
            if line.starts_with("sha256 = ") {
                format!("{line}\nversion = 1\nid = 7\n")
            } else {
                format!("{line}\n")
            }
        })
        .collect();
    krate.write("schemas/kafka-schema.lock", &applied);
}

#[test]
fn check_passes_when_exports_lock_and_line_agree() {
    let krate = orders();
    let run = krate.tool(&["check"]).ok();
    assert!(
        run.stdout.contains("check passed: 1 subject(s)"),
        "{}",
        run.stdout
    );
    krate.tool(&["prepare", "--check"]).ok();
}

#[test]
fn check_fails_when_the_type_moved_ahead_of_its_migrations() {
    let krate = orders();
    krate.exporting(&[("com.acme.Order", "avsc", ORDER_V2)]);
    let stderr = krate.tool(&["check"]).fails();
    assert!(
        stderr.contains(
            "the exported schema of record `com.acme.Order` differs from `0001_create_order.avsc`, \
             the newest migration of subject `orders-value`: describe the change with \
             `cargo kafka-schema migrate add orders-value <description>`"
        ),
        "{stderr}"
    );
    krate.exporting(&[]);
    let stderr = krate.tool(&["check"]).fails();
    assert!(
        stderr.contains("no type in the crate exported record `com.acme.Order`"),
        "{stderr}"
    );
}

#[test]
fn a_hand_edited_pending_migration_makes_the_lock_stale_until_prepare() {
    let krate = orders();
    krate.write("schemas/orders-value/0001_create_order.avsc", ORDER_V2);
    krate.exporting(&[("com.acme.Order", "avsc", ORDER_V2)]);
    let stderr = krate.tool(&["check"]).fails();
    assert!(
        stderr.contains(
            "migration `0001_create_order.avsc` of subject `orders-value` differs from the \
             checksum the lock records"
        ),
        "{stderr}"
    );
    let stderr = krate.tool(&["prepare", "--check"]).fails();
    assert!(
        stderr.contains("refresh it with `cargo kafka-schema prepare`"),
        "{stderr}"
    );
    krate.tool(&["prepare"]).ok();
    krate.tool(&["prepare", "--check"]).ok();
    krate.tool(&["check"]).ok();
}

#[test]
fn a_registered_migration_is_immutable() {
    let krate = orders();
    mark_applied(&krate);
    krate.tool(&["check"]).ok();
    krate.write("schemas/orders-value/0001_create_order.avsc", ORDER_V2);
    for command in [&["check"][..], &["prepare"][..], &["migrate", "status"][..]] {
        let stderr = krate.tool(command).fails();
        assert!(
            stderr.contains(
                "migration `0001_create_order.avsc` of subject `orders-value` changed after it \
                 was registered"
            ),
            "{command:?}: {stderr}"
        );
    }
}

#[test]
fn a_migration_added_by_hand_is_not_in_the_lock() {
    let krate = orders();
    krate.write("schemas/orders-value/0002_by_hand.avsc", ORDER_V2);
    krate.exporting(&[("com.acme.Order", "avsc", ORDER_V2)]);
    let stderr = krate.tool(&["check"]).fails();
    assert!(
        stderr
            .contains("migration `0002_by_hand.avsc` of subject `orders-value` is not in the lock"),
        "{stderr}"
    );
}

#[test]
fn status_reports_the_lock_offline() {
    let krate = orders();
    let run = krate.tool(&["migrate", "status"]).ok();
    assert!(run.stdout.contains("registry: none"), "{}", run.stdout);
    assert!(
        run.stdout
            .contains("orders-value (avro, level not declared, owned)"),
        "{}",
        run.stdout
    );
    assert!(
        run.stdout.contains("  0001_create_order.avsc  pending"),
        "{}",
        run.stdout
    );
    mark_applied(&krate);
    let run = krate.tool(&["migrate", "status"]).ok();
    assert!(
        run.stdout
            .contains("  0001_create_order.avsc  version 1, id 7"),
        "{}",
        run.stdout
    );
}

#[test]
fn check_compares_a_protobuf_source_with_its_newest_migration() {
    let krate = Crate::new();
    krate.write("proto/payment.proto", PAYMENT);
    krate
        .tool(&[
            "migrate",
            "add",
            "payments-value",
            "init",
            "--source",
            "proto/payment.proto",
        ])
        .ok();
    krate.tool(&["check"]).ok();
    krate.write(
        "proto/payment.proto",
        &PAYMENT.replace("id = 1;", "id = 1; string note = 2;"),
    );
    let stderr = krate.tool(&["check"]).fails();
    assert!(
        stderr.contains(
            "`proto/payment.proto` differs from `0001_init.proto`, the newest migration of \
             subject `payments-value`"
        ),
        "{stderr}"
    );
}

#[test]
fn check_holds_the_whole_line_to_the_level() {
    let krate = orders();
    // A hand-written second migration that breaks BACKWARD, recorded by prepare.
    let qty = ORDER_V1.replace("}]}", r#"},{"name":"qty","type":"int"}]}"#);
    krate.write("schemas/orders-value/0002_add_qty.avsc", &qty);
    krate.exporting(&[("com.acme.Order", "avsc", &qty)]);
    krate.tool(&["prepare"]).ok();
    let stderr = krate.tool(&["check"]).fails();
    assert!(
        stderr.contains("migration `0002_add_qty.avsc` of subject `orders-value` breaks BACKWARD"),
        "{stderr}"
    );
}
