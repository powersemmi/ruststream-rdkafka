//! What the `Schema` derive's generated code reaches. Machinery: never named by hand.

use std::fs;
use std::path::Path;

#[cfg(feature = "avro")]
pub use apache_avro::AvroSchema;
#[cfg(feature = "avro")]
pub use serde_json;

pub use super::avro::{
    AvroFields, AvroForm, Carries, EnumForm, FieldForm, FixedForm, Form, Member, Name, Reads,
    RecordForm, Shape, Site, assert_equals, assert_reads, kind,
};
pub use super::record::{Record, RecordKind, RecordSet};

/// Writes `schema` as `<dir>/<record>.<extension>`: the export the migrations tool reads.
///
/// # Panics
///
/// When the directory cannot be created or the file cannot be written: the export runs as a
/// test, and a failed write fails it.
#[allow(clippy::needless_pass_by_value)]
pub fn export(dir: &str, record: &str, extension: &str, schema: String) {
    let dir = Path::new(dir);
    fs::create_dir_all(dir)
        .unwrap_or_else(|err| panic!("creating the export directory `{}`: {err}", dir.display()));
    let path = dir.join(format!("{record}.{extension}"));
    fs::write(&path, schema)
        .unwrap_or_else(|err| panic!("writing the export `{}`: {err}", path.display()));
}
