//! Avro transcoding for the Schema Registry middleware.
//!
//! With the `avro` feature, the registry middleware converts Confluent-framed Avro both
//! ways: incoming datums decode against their registry schema and arrive at handlers as
//! plain JSON; outgoing JSON payloads serialize against the destination subject's schema
//! when the [`SchemaFrame`](crate::SchemaFrame) publish layer finds an Avro schema there.
//! Handler types stay plain serde structs; the [`AvroSchema`] derive (re-exported) is only
//! needed to register a subject straight from a type
//! ([`SchemaRegistry::register_avro`](crate::SchemaRegistry::register_avro)).

pub use apache_avro::AvroSchema;

use crate::error::KafkaError;
use crate::schema_registry::{RegisteredSchema, SchemaRegistry};

/// Decodes an Avro datum against its registry schema and re-encodes it as JSON.
pub(crate) fn avro_to_json(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    datum: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let writer = registry.parsed_avro(schema)?;
    let mut cursor = datum;
    let value = apache_avro::from_avro_datum(&writer, &mut cursor, None)
        .map_err(KafkaError::schema_registry)?;
    let json: serde_json::Value =
        apache_avro::from_value(&value).map_err(KafkaError::schema_registry)?;
    serde_json::to_vec(&json).map_err(KafkaError::schema_registry)
}

/// Serializes a JSON document as an Avro datum against the subject's registry schema.
pub(crate) fn json_to_avro(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    payload: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let parsed = registry.parsed_avro(schema)?;
    let json: serde_json::Value =
        serde_json::from_slice(payload).map_err(KafkaError::schema_registry)?;
    let value = apache_avro::to_value(json)
        .map_err(KafkaError::schema_registry)?
        .resolve(&parsed)
        .map_err(KafkaError::schema_registry)?;
    apache_avro::to_avro_datum(&parsed, value).map_err(KafkaError::schema_registry)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
    struct Order {
        id: i64,
        item: String,
    }

    async fn registry_with_order(id: u32) -> (MockServer, SchemaRegistry, RegisteredSchema) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": id })))
            .mount(&server)
            .await;
        let sr = SchemaRegistry::new(server.uri());
        sr.register_avro::<Order>("orders-value")
            .await
            .expect("register");
        let schema = sr.cached_subject("orders-value").expect("cached");
        (server, sr, (*schema).clone())
    }

    #[tokio::test]
    async fn json_avro_json_roundtrips() {
        let (_server, sr, schema) = registry_with_order(7).await;
        let json = br#"{"id":42,"item":"anvil"}"#;

        let datum = json_to_avro(&sr, &schema, json).expect("encode");
        assert_ne!(datum.as_slice(), json, "the wire form is Avro, not JSON");

        let back = avro_to_json(&sr, &schema, &datum).expect("decode");
        let order: Order = serde_json::from_slice(&back).expect("deserialize");
        assert_eq!(
            order,
            Order {
                id: 42,
                item: "anvil".to_owned(),
            },
        );
    }

    #[tokio::test]
    async fn schema_mismatches_error_clearly() {
        let (_server, sr, schema) = registry_with_order(7).await;
        let err = json_to_avro(&sr, &schema, br#"{"id":"not-a-number"}"#)
            .expect_err("a document violating the schema must fail");
        assert!(matches!(err, KafkaError::SchemaRegistry(_)));
    }
}
