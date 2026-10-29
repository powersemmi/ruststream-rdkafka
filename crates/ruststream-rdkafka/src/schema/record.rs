//! What the `Schema` derive tells the runtime about a type: the record it is, or for a set of
//! input models the record of each variant.

/// A type the `Schema` derive bound to a schema record: its record's full name, the anchor of
/// its migrations.
///
/// The derive implements it: for an Avro type the full name `AvroSchema` gives it
/// (`com.acme.orders.Order`), for a JSON Schema type the namespace of `#[schema(namespace = ..)]`
/// plus the type name.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is bound to no schema record",
    label = "derive `Schema` on this type",
    note = "`#[derive(Schema)]` names the record a migration in `schemas/` declares"
)]
pub trait Record {
    /// The record's full name.
    const RECORD: &'static str;
}

/// A set of input models (`#[derive(InMessages)]`) whose variants each carry a record: the
/// record of each variant, in the order of the variants.
///
/// `#[derive(Schema)]` on the set implements it, reading each record from the variant's model.
pub trait RecordSet {
    /// The records of the variants, in the order of the variants.
    const RECORDS: &'static [&'static str];
}

/// The kind of a set of input models (`OrderEventKind` for `OrderEvent`), mapped from the
/// record each variant carries: the discriminator picks the variant whose record has the full
/// name the delivery's schema declares.
///
/// `#[derive(Schema)]` on the set implements it for the set's kind.
pub trait RecordKind: Copy + 'static {
    /// Each variant's record, with the kind that names the variant.
    const KINDS: &'static [(&'static str, Self)];

    /// The kind whose variant carries the record `record`, by its full name.
    #[must_use]
    fn of_record(record: &str) -> Option<Self> {
        Self::KINDS
            .iter()
            .find(|(name, _)| *name == record)
            .map(|(_, kind)| *kind)
    }
}
