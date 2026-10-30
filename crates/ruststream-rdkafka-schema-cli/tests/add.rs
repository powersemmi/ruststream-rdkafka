//! `migrate add`: the next migration from the crate's current schema, checked locally.

use crate::support::Crate;

const PAYMENT_V1: &str = "syntax = \"proto3\";\npackage acme.payments;\n\nmessage Payment {\n  int64 id = 1;\n  string item = 2;\n}\n";

const ORDER_V1: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"}]}"#;
const ORDER_V2: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"note","type":["null","string"],"default":null}]}"#;
const ORDER_QTY: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"qty","type":"int"}]}"#;

fn payments() -> Crate {
    let krate = Crate::new();
    krate.write("proto/payments.proto", PAYMENT_V1);
    krate
        .tool(&[
            "migrate",
            "add",
            "payments-value",
            "init",
            "--source",
            "proto/payments.proto",
            "--compatibility",
            "BACKWARD_TRANSITIVE",
        ])
        .ok();
    krate
}

#[test]
fn a_protobuf_subject_starts_with_a_snapshot_of_its_source() {
    let krate = payments();
    assert_eq!(
        krate.read("schemas/payments-value/subject.toml"),
        "format = \"protobuf\"\ncompatibility = \"BACKWARD_TRANSITIVE\"\n\
         record = \"acme.payments.Payment\"\nsource = \"proto/payments.proto\"\n"
    );
    assert_eq!(
        krate.read("schemas/payments-value/0001_init.proto"),
        PAYMENT_V1
    );
    let lock = krate.read("schemas/kafka-schema.lock");
    assert!(lock.contains("name = \"payments-value\""), "{lock}");
    assert!(
        lock.contains("file = \"0001_init.proto\"\nsha256 = \""),
        "{lock}"
    );
    assert!(!lock.contains("version = 1\nid"), "pending: {lock}");

    let run = krate
        .tool(&["migrate", "add", "payments-value", "again"])
        .ok();
    assert!(
        run.stdout
            .contains("equals its newest migration `0001_init.proto`; nothing to add"),
        "{}",
        run.stdout
    );
    assert!(!krate.exists("schemas/payments-value/0002_again.proto"));
}

#[test]
fn a_renumbered_protobuf_field_is_refused_until_its_number_is_reserved() {
    let krate = payments();
    krate.write(
        "proto/payments.proto",
        &PAYMENT_V1.replace("item = 2", "item = 3"),
    );
    let stderr = krate
        .tool(&["migrate", "add", "payments-value", "move_item"])
        .fails();
    assert!(
        stderr.contains(
            "migration `0002_move_item.proto` of subject `payments-value` drops field `item = 2` \
             of message `acme.payments.Payment`"
        ),
        "{stderr}"
    );
    assert!(
        !krate.exists("schemas/payments-value/0002_move_item.proto"),
        "nothing written"
    );

    krate.write(
        "proto/payments.proto",
        &PAYMENT_V1.replace("item = 2;", "item = 3;\n  reserved 2;"),
    );
    krate
        .tool(&["migrate", "add", "payments-value", "move_item"])
        .ok();
    assert!(krate.exists("schemas/payments-value/0002_move_item.proto"));
}

#[test]
fn an_avro_subject_takes_the_exported_schema_and_its_level_holds() {
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
    assert_eq!(
        krate.read("schemas/orders-value/subject.toml"),
        "format = \"avro\"\nrecord = \"com.acme.Order\"\n"
    );
    let written = krate.read("schemas/orders-value/0001_create_order.avsc");
    assert!(
        written.starts_with("{\n  \"fields\": ["),
        "pretty, sorted: {written}"
    );

    // A field without a default: a reader of the new schema cannot read what 0001 wrote.
    krate.exporting(&[("com.acme.Order", "avsc", ORDER_QTY)]);
    let stderr = krate
        .tool(&["migrate", "add", "orders-value", "add_qty"])
        .fails();
    assert!(
        stderr.contains(
            "migration `0002_add_qty.avsc` of subject `orders-value` breaks BACKWARD: a reader \
             of it cannot read what `0001_create_order.avsc` wrote"
        ),
        "{stderr}"
    );
    assert!(!krate.exists("schemas/orders-value/0002_add_qty.avsc"));

    krate.exporting(&[("com.acme.Order", "avsc", ORDER_V2)]);
    krate
        .tool(&["migrate", "add", "orders-value", "add_note"])
        .ok();
    assert!(krate.exists("schemas/orders-value/0002_add_note.avsc"));
    let lock = krate.read("schemas/kafka-schema.lock");
    assert!(lock.contains("file = \"0002_add_note.avsc\""), "{lock}");
}

#[test]
fn a_new_subject_needs_its_record_and_an_exported_type() {
    let krate = Crate::new();
    let stderr = krate
        .tool(&["migrate", "add", "orders-value", "create_order"])
        .fails();
    assert!(
        stderr
            .contains("subject `orders-value` is new: name the record it declares with `--record"),
        "{stderr}"
    );
    let stderr = krate
        .tool(&[
            "migrate",
            "add",
            "orders-value",
            "create_order",
            "--record",
            "com.acme.Order",
        ])
        .fails();
    assert!(
        stderr.contains("no type in the crate exported record `com.acme.Order`"),
        "{stderr}"
    );
    let stderr = krate
        .tool(&["migrate", "add", "orders-value", "Create-Order"])
        .fails();
    assert!(
        stderr.contains("`Create-Order` cannot name a migration"),
        "{stderr}"
    );
}

#[test]
fn a_json_subject_at_backward_must_close_its_objects() {
    let open = r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"Order","type":"object","properties":{"id":{"type":"integer","format":"int64"}},"required":["id"]}"#;
    let krate = Crate::new();
    krate.exporting(&[("com.acme.Order", "json", open)]);
    let stderr = krate
        .tool(&[
            "migrate",
            "add",
            "orders-value",
            "create_order",
            "--record",
            "com.acme.Order",
        ])
        .fails();
    assert!(stderr.contains("leaves the object at `#` open"), "{stderr}");
    assert!(
        stderr.contains("`#[serde(deny_unknown_fields)]`"),
        "{stderr}"
    );
    assert!(!krate.exists("schemas/orders-value"), "nothing written");

    let closed = open.replace(
        r#""required":["id"]"#,
        r#""required":["id"],"additionalProperties":false"#,
    );
    krate.exporting(&[("com.acme.Order", "json", &closed)]);
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
    assert_eq!(
        krate.read("schemas/orders-value/subject.toml"),
        "format = \"json\"\nrecord = \"com.acme.Order\"\n"
    );

    let invalid = closed.replace(r#""type":"integer""#, r#""type":"integr""#);
    krate.exporting(&[("com.acme.Order", "json", &invalid)]);
    let stderr = krate
        .tool(&["migrate", "add", "orders-value", "typo"])
        .fails();
    assert!(
        stderr.contains("is not a valid JSON Schema: at `/properties/id/type`"),
        "{stderr}"
    );
}

#[test]
fn an_external_subject_arrives_by_import_only() {
    let krate = Crate::new();
    krate.write(
        "schemas/inventory-value/subject.toml",
        "format = \"avro\"\nowner = \"external\"\n",
    );
    krate.write("schemas/inventory-value/0001.avsc", ORDER_V1);
    let stderr = krate
        .tool(&["migrate", "add", "inventory-value", "x"])
        .fails();
    assert!(
        stderr.contains("subject `inventory-value` is external"),
        "{stderr}"
    );
    let stderr = krate.tool(&["migrate", "run"]).fails();
    assert!(
        stderr.contains("`migrate run` needs a registry"),
        "{stderr}"
    );
    let stderr = krate
        .tool(&["migrate", "import", "inventory-value"])
        .fails();
    assert!(
        stderr.contains("`migrate import` needs a registry"),
        "{stderr}"
    );
}
