//! The tool against a real registry: registration, the registry's own compatibility check,
//! drift, version gaps and references. Each scenario names its subjects uniquely, so runs share
//! the registry without meeting.

use ruststream_rdkafka::SchemaRegistry;
use ruststream_rdkafka::schema_registry::SchemaType;
use ruststream_rdkafka::schema_registry::admin::{CompatibilityLevel, RegistryAdmin, SchemaDraft};

use crate::support::{Crate, registry_url, unique};

const ORDER_V1: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"}]}"#;
const ORDER_V2: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"qty","type":"int","default":0}]}"#;
const ORDER_V3: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"qty","type":"int"}]}"#;
const ORDER_NOTE: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"note","type":["null","string"],"default":null}]}"#;

fn admin(url: &str) -> RegistryAdmin {
    SchemaRegistry::new(url).admin().expect("shipped client")
}

/// An Avro subject whose migrations are written by hand.
fn avro_subject(krate: &Crate, subject: &str, level: &str, migrations: &[&str]) {
    krate.write(
        &format!("schemas/{subject}/subject.toml"),
        &format!("format = \"avro\"\ncompatibility = \"{level}\"\nrecord = \"com.acme.Order\"\n"),
    );
    for (number, text) in (1..).zip(migrations) {
        krate.write(&format!("schemas/{subject}/{number:04}_m.avsc"), text);
    }
}

/// Records hand-written migrations in the lock, as pending.
fn offline_prepare(krate: &Crate) {
    krate.tool(&["--offline", "prepare"]).ok();
}

/// Deletes a version softly: it leaves the version list and the content lookup.
async fn soft_delete(url: &str, subject: &str, version: u32) {
    reqwest::Client::new()
        .delete(format!("{url}/subjects/{subject}/versions/{version}"))
        .send()
        .await
        .expect("delete")
        .error_for_status()
        .expect("deleted");
}

/// The checksum the lock records for `file`.
fn sha_of(lock: &str, file: &str) -> String {
    let prefix = format!("file = \"{file}\"\nsha256 = \"");
    let start = lock.find(&prefix).expect("entry") + prefix.len();
    lock[start..start + 64].to_owned()
}

#[tokio::test]
async fn run_registers_in_order_sets_the_level_and_the_lock_agrees() {
    let Some(url) = registry_url() else { return };
    let subject = unique("payments");
    let krate = Crate::new().online(&url);
    let v1 = "syntax = \"proto3\";\npackage acme;\nmessage Payment {\n  int64 id = 1;\n}\n";
    krate.write("proto/payment.proto", v1);
    krate
        .tool(&[
            "--offline",
            "migrate",
            "add",
            &subject,
            "init",
            "--source",
            "proto/payment.proto",
            "--compatibility",
            "BACKWARD_TRANSITIVE",
        ])
        .ok();

    let run = krate.tool(&["migrate", "run"]).ok();
    assert!(
        run.stdout
            .contains("registered `0001_init.proto` as version 1"),
        "{}",
        run.stdout
    );
    let admin = admin(&url);
    assert_eq!(
        admin.compatibility(&subject).await.expect("level"),
        Some(CompatibilityLevel::BackwardTransitive)
    );
    let lock = krate.read("schemas/kafka-schema.lock");
    assert!(lock.contains("registry-versions = [1]"), "{lock}");
    assert!(lock.contains("version = 1\nid = "), "{lock}");
    krate.tool(&["prepare", "--check"]).ok();
    krate.tool(&["check"]).ok();

    krate.write(
        "proto/payment.proto",
        &v1.replace("id = 1;", "id = 1;\n  string note = 2;"),
    );
    krate.tool(&["migrate", "add", &subject, "add_note"]).ok();
    let dry = krate.tool(&["migrate", "run", "--dry-run"]).ok();
    assert!(
        dry.stdout.contains("would register `0002_add_note.proto`"),
        "{}",
        dry.stdout
    );
    assert_eq!(admin.versions(&subject).await.expect("versions"), [1]);
    krate.tool(&["migrate", "run"]).ok();
    assert_eq!(admin.versions(&subject).await.expect("versions"), [1, 2]);
    let status = krate.tool(&["migrate", "status"]).ok();
    assert!(
        status
            .stdout
            .contains("0002_add_note.proto  version 2, id "),
        "{}",
        status.stdout
    );
    krate.tool(&["prepare", "--check"]).ok();
}

#[tokio::test]
async fn the_registry_check_covers_every_version_and_prints_its_messages() {
    let Some(url) = registry_url() else { return };
    let subject = unique("orders");
    let krate = Crate::new().online(&url);
    // 0003 reads what 0002 wrote (it carries `qty`), and not what 0001 wrote: a check against
    // the latest version alone passes it.
    avro_subject(
        &krate,
        &subject,
        "BACKWARD_TRANSITIVE",
        &[ORDER_V1, ORDER_V2, ORDER_V3],
    );
    offline_prepare(&krate);

    let stderr = krate.tool(&["migrate", "run"]).fails();
    assert!(
        stderr.contains("registry: "),
        "the verbose messages: {stderr}"
    );
    assert!(
        stderr.contains("READER_FIELD_MISSING_DEFAULT_VALUE"),
        "{stderr}"
    );
    assert!(
        stderr.contains(&format!(
            "the registry refuses migration `0003_m.avsc` of subject `{subject}`"
        )),
        "{stderr}"
    );
    assert_eq!(
        admin(&url).versions(&subject).await.expect("versions"),
        [1, 2]
    );
    let lock = krate.read("schemas/kafka-schema.lock");
    let pending = format!(
        "file = \"0003_m.avsc\"\nsha256 = \"{}\"\n",
        sha_of(&lock, "0003_m.avsc")
    );
    assert!(lock.ends_with(&pending), "0003 stays pending: {lock}");
}

#[tokio::test]
async fn a_version_no_migration_describes_is_drift_until_imported() {
    let Some(url) = registry_url() else { return };
    let subject = unique("orders");
    let krate = Crate::new().online(&url);
    avro_subject(&krate, &subject, "BACKWARD", &[ORDER_V1]);
    offline_prepare(&krate);
    krate.tool(&["migrate", "run"]).ok();
    admin(&url)
        .register(&subject, &SchemaDraft::new(SchemaType::Avro, ORDER_NOTE))
        .await
        .expect("registered behind the tool's back");

    let expected = format!(
        "the registry holds version 2 of subject `{subject}`, which no migration describes"
    );
    for command in [
        &["prepare"][..],
        &["prepare", "--check"][..],
        &["migrate", "status"][..],
        &["migrate", "run"][..],
    ] {
        let stderr = krate.tool(command).fails();
        assert!(stderr.contains(&expected), "{command:?}: {stderr}");
    }
    let run = krate.tool(&["migrate", "import", &subject]).ok();
    assert!(
        run.stdout.contains("0002.avsc` (version 2)"),
        "{}",
        run.stdout
    );
    krate.tool(&["prepare", "--check"]).ok();
    krate.tool(&["migrate", "status"]).ok();
}

#[tokio::test]
async fn a_registered_migration_the_registry_lost_is_drift() {
    let Some(url) = registry_url() else { return };
    let subject = unique("orders");
    let krate = Crate::new().online(&url);
    avro_subject(&krate, &subject, "BACKWARD", &[ORDER_V1, ORDER_NOTE]);
    offline_prepare(&krate);
    krate.tool(&["migrate", "run"]).ok();
    soft_delete(&url, &subject, 2).await;

    let stderr = krate.tool(&["prepare"]).fails();
    assert!(
        stderr.contains(&format!(
            "migration `0002_m.avsc` of subject `{subject}` is recorded as version 2, and the \
             registry no longer holds it"
        )),
        "{stderr}"
    );
}

#[tokio::test]
async fn an_import_keeps_the_gaps_the_registry_left() {
    let Some(url) = registry_url() else { return };
    let subject = unique("inventory");
    let admin = admin(&url);
    for schema in [ORDER_V1, ORDER_NOTE] {
        admin
            .register(&subject, &SchemaDraft::new(SchemaType::Avro, schema))
            .await
            .expect("register");
    }
    soft_delete(&url, &subject, 2).await;
    admin
        .register(&subject, &SchemaDraft::new(SchemaType::Avro, ORDER_V2))
        .await
        .expect("register after the soft delete");
    assert_eq!(admin.versions(&subject).await.expect("versions"), [1, 3]);

    let krate = Crate::new().online(&url);
    krate.tool(&["migrate", "import", &subject]).ok();
    assert_eq!(
        krate.read(&format!("schemas/{subject}/subject.toml")),
        "format = \"avro\"\nowner = \"external\"\nrecord = \"com.acme.Order\"\n"
    );
    assert!(krate.exists(&format!("schemas/{subject}/0001.avsc")));
    assert!(krate.exists(&format!("schemas/{subject}/0002.avsc")));
    let lock = krate.read("schemas/kafka-schema.lock");
    assert!(lock.contains("registry-versions = [1, 3]"), "{lock}");
    assert!(lock.contains("version = 3\nid = "), "{lock}");
    krate.tool(&["prepare", "--check"]).ok();
    krate.tool(&["check"]).ok();
}

#[tokio::test]
async fn referenced_subjects_register_first_and_resolve_by_reference() {
    let Some(url) = registry_url() else { return };
    // `a-` sorts the referencing subject first, so only the reference order puts it second.
    let payments = unique("a-payments");
    let money = unique("money");
    let krate = Crate::new().online(&url);
    krate.write(
        &format!("schemas/{money}/subject.toml"),
        "format = \"protobuf\"\nrecord = \"acme.Money\"\n",
    );
    krate.write(
        &format!("schemas/{money}/0001_init.proto"),
        "syntax = \"proto3\";\npackage acme;\nmessage Money { int64 cents = 1; }\n",
    );
    krate.write(
        &format!("schemas/{payments}/subject.toml"),
        &format!(
            "format = \"protobuf\"\nrecord = \"acme.Payment\"\n\n[[references]]\n\
             name = \"money.proto\"\nsubject = \"{money}\"\n"
        ),
    );
    krate.write(
        &format!("schemas/{payments}/0001_init.proto"),
        "syntax = \"proto3\";\npackage acme;\nimport \"money.proto\";\n\
         message Payment { int64 id = 1; Money amount = 2; }\n",
    );
    offline_prepare(&krate);
    let run = krate.tool(&["migrate", "run"]).ok();
    let money_at = run
        .stdout
        .find(&format!("{money}: registered"))
        .expect("money registered");
    let payments_at = run
        .stdout
        .find(&format!("{payments}: registered"))
        .expect("payments registered");
    assert!(money_at < payments_at, "{}", run.stdout);
    krate.tool(&["prepare", "--check"]).ok();
}
