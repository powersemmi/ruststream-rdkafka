//! The export through the derive's own hidden test, end to end.

use crate::support::Crate;

/// Runs `migrate add` and `check` on a crate whose type derives `Schema` with the real derive.
///
/// Ignored until the derive emits its export test (the `Schema` derive's export mode); run it
/// with `cargo test -p ruststream-rdkafka-schema-cli -- --ignored` once it does. It builds
/// `ruststream-rdkafka` into the fixture crate, which takes a few minutes the first time.
#[test]
#[ignore = "needs the export test the `Schema` derive emits"]
fn the_derive_exports_the_runtime_schema() {
    let rdkafka = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../ruststream-rdkafka")
        .canonicalize()
        .expect("the crate beside this one");
    let krate = Crate::new();
    krate.write(
        "Cargo.toml",
        &format!(
            "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n\
             [workspace]\n\n[dependencies]\n\
             ruststream-rdkafka = {{ path = {rdkafka:?}, features = [\"avro\", \"schema-check\"] }}\n\
             apache-avro = {{ version = \"0.22\", features = [\"derive\"] }}\n\
             serde = {{ version = \"1\", features = [\"derive\"] }}\n"
        ),
    );
    krate.write(
        "src/lib.rs",
        "//! A service.\n\nuse apache_avro::AvroSchema;\nuse ruststream_rdkafka::schema::Schema;\n\
         use serde::{Deserialize, Serialize};\n\n/// An order.\n\
         #[derive(Serialize, Deserialize, AvroSchema, Schema)]\n\
         #[avro(namespace = \"com.acme.orders\")]\npub struct Order {\n    /// Its id.\n    pub id: i64,\n}\n",
    );
    krate
        .tool(&[
            "migrate",
            "add",
            "orders-value",
            "create_order",
            "--record",
            "com.acme.orders.Order",
        ])
        .ok();
    let migration = krate.read("schemas/orders-value/0001_create_order.avsc");
    assert!(migration.contains("\"com.acme.orders\""), "{migration}");
    krate.tool(&["check"]).ok();
}
