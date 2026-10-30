---
sidebar_position: 2
title: JSON Schema check
description: "#[schema(namespace = ..)] compares a type's serde shape with the JSON Schema of its subject, in both directions for an owned subject."
---

# JSON Schema check

`#[derive(Schema)]` with `#[schema(namespace = ..)]` compares the type's serde shape with the JSON Schema of every subject that declares it.

The claim is about serde, not about the struct's fields.
Every value the type serializes is valid under the subject's schema, and every value the schema admits deserializes into the type.
An owned subject is checked in both directions; an external one in the second only, because the service only reads it.
A JSON Schema names no record, so the subject's `subject.toml` records the anchor, `record = "com.acme.carts.Cart"`: the namespace plus the type's name.

The derive reads serde's own attributes with `serde_derive_internals`: `rename`, `rename_all`, `default`, `skip`, `skip_serializing_if`, `flatten`, `deny_unknown_fields`, `transparent` and enum tagging mean what serde makes of them.
The standard types carry their shape: integers, floats, `bool`, `String`, `Option`, `Vec`, string maps and `serde_json::Value`, and with the `uuid` and `chrono` features their types.
A field of another type contributes that type's shape, so nested types keep their own attributes.

What is compared:
- `properties` and `required`: property names equal the serde names; a required property is written on every value, an optional one deserializes when absent.
- null: the schema admits null exactly when the field is an `Option`.
- `integer` and `number`: the range from `format` (`int8` .. `uint64`), narrowed by `minimum` and `maximum`; a bare `integer` reads as signed 64-bit.
- `string` and its `format` (`uuid`, `date-time`), and an `enum` of strings after `rename_all`.
- `oneOf` from an externally, internally or adjacently tagged enum, branch for variant, payloads included.
- `items`, `additionalProperties` against `deny_unknown_fields` and flattened maps, `$ref` into `$defs` or `definitions`, and `$ref` to another subject through `[[references]]` in `subject.toml`.

A construct a Rust type cannot promise (`pattern`, `minLength`, `multipleOf`, `not`, `if`, `patternProperties`, `oneOf` against an untagged enum, tuples, a recursive `$ref`) is a compile error that names the keyword and its JSON pointer.
`#[schema(unchecked)]` on the field then checks its presence and optionality only, and `cargo kafka-schema check` still compares the exported schema with the migration exactly.
Drafts 04 to 2020-12 are read.

## Example

`Cart` and `Item` match the JSON Schema of `carts-value`.

{/* single-form: message types are plain derives, the same in both forms */}
```rust reference="examples/kafka_schema_json.rs#model"
```

The first difference fails the build with its path:

```text
error[E0080]: evaluation panicked: `Cart` does not match subject `carts-value`
              (migration 0001_create_cart): Cart.items[].qty: the integer type writes values
              outside the schema's range
```
