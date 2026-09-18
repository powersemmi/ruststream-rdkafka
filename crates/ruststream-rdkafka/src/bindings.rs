//! What this crate contributes to the generated `AsyncAPI` document, in Kafka's own vocabulary.
//!
//! The specification calls it the `kafka` binding; these are its bodies, built from what a
//! descriptor, a broker or a publish policy already holds. Nothing here needs a connection, and
//! nothing here carries a credential: the document is published and shared.

use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;

/// The version of the `kafka` binding these bodies are written against.
const BINDING_VERSION: &str = "0.5.0";

/// Wraps `body` as the `kafka` binding, or says nothing when it cannot be built. A broker never
/// holds a service up over a description of itself.
fn kafka<T: Serialize>(body: &T) -> Bindings {
    Binding::new("kafka", BINDING_VERSION, body)
        .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
}

/// A one-value Schema Object, which is how the Kafka binding carries an identifier.
#[derive(Debug, Serialize)]
struct Constant {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(rename = "enum")]
    values: [String; 1],
}

impl Constant {
    fn new(value: &str) -> Self {
        Self {
            kind: "string",
            values: [value.to_owned()],
        }
    }
}

/// The server binding: where the schema registry lives and whose it is.
#[derive(Debug, Serialize)]
struct Server {
    #[serde(rename = "schemaRegistryUrl")]
    schema_registry_url: String,
    #[serde(rename = "schemaRegistryVendor")]
    schema_registry_vendor: &'static str,
}

/// The channel binding: the Kafka topic behind the channel.
#[derive(Debug, Serialize)]
struct Channel<'a> {
    topic: &'a str,
}

/// The operation binding: who consumes, and under which client identity.
#[derive(Debug, Serialize)]
struct Operation {
    #[serde(rename = "groupId", skip_serializing_if = "Option::is_none")]
    group_id: Option<Constant>,
    #[serde(rename = "clientId", skip_serializing_if = "Option::is_none")]
    client_id: Option<Constant>,
}

/// The message binding: where a registry-backed payload keeps its schema id.
#[cfg(feature = "schema-registry")]
#[derive(Debug, Serialize)]
struct Message {
    #[serde(rename = "schemaIdLocation")]
    id_location: &'static str,
    #[serde(rename = "schemaIdPayloadEncoding")]
    id_payload_encoding: &'static str,
    #[serde(rename = "schemaLookupStrategy")]
    lookup_strategy: &'static str,
}

/// The registry coordinate of a server, reported only where a registry is configured.
///
/// The vendor is `confluent` because the wire format this crate writes is Confluent's envelope,
/// not because the deployment says so.
pub(crate) fn server(registry_url: Option<&str>) -> Bindings {
    let Some(url) = registry_url else {
        return Bindings::new();
    };
    kafka(&Server {
        schema_registry_url: without_userinfo(url),
        schema_registry_vendor: "confluent",
    })
}

/// The topic a channel stands for. Only a subscription that reads exactly one topic has one; a
/// list and a regex read many and name none of them here.
pub(crate) fn channel(topic: &str) -> Bindings {
    kafka(&Channel { topic })
}

/// The consumer group and client id of a `receive` operation, from what the descriptor carries.
///
/// A group left to `KafkaBroker::default_group` does not appear: the document is built from the
/// descriptor alone, which never sees the broker it will be mounted on.
pub(crate) fn operation(group: Option<&str>, config: &[(String, String)]) -> Bindings {
    let client_id = config
        .iter()
        .rev()
        .find(|(key, _)| key == "client.id")
        .map(|(_, value)| Constant::new(value));
    let group_id = group.map(Constant::new);
    if group_id.is_none() && client_id.is_none() {
        return Bindings::new();
    }
    kafka(&Operation {
        group_id,
        client_id,
    })
}

/// How a registry-backed publisher frames its payloads: the Confluent envelope puts the schema
/// id in the payload, and the naming strategy says which subject that id was registered under.
#[cfg(feature = "schema-registry")]
pub(crate) fn message(strategy: crate::schema_registry::SubjectStrategy) -> Bindings {
    use crate::schema_registry::SubjectStrategy;

    kafka(&Message {
        id_location: "payload",
        id_payload_encoding: "confluent",
        lookup_strategy: match strategy {
            SubjectStrategy::TopicName => "TopicNameStrategy",
            SubjectStrategy::RecordName => "RecordNameStrategy",
            SubjectStrategy::TopicRecordName => "TopicRecordNameStrategy",
        },
    })
}

/// Drops the userinfo a URL may carry, the way `ServerSpec::host_from_url` does for a broker
/// address: `http://user:secret@registry:8081` describes the same registry as
/// `http://registry:8081`, and only one of the two is safe to publish.
fn without_userinfo(url: &str) -> String {
    let (scheme, rest) = url
        .split_once("://")
        .map_or(("", url), |(scheme, rest)| (scheme, rest));
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let trimmed = rest[..authority_end].rfind('@').map_or_else(
        || rest.to_owned(),
        |at| format!("{}{}", &rest[at + 1..authority_end], &rest[authority_end..]),
    );
    if scheme.is_empty() {
        trimmed
    } else {
        format!("{scheme}://{trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registry_url_reaches_the_document_without_its_password() {
        let bindings = server(Some("http://svc:hunter2@registry.internal:8081"));
        let json = serde_json::to_string(&bindings).expect("bindings serialize");
        assert!(
            json.contains("http://registry.internal:8081"),
            "the coordinate must survive, got: {json}"
        );
        assert!(
            !json.contains("hunter2"),
            "a published document must not carry the registry password, got: {json}"
        );
    }

    #[test]
    fn a_url_without_userinfo_is_reported_as_it_stands() {
        assert_eq!(
            without_userinfo("https://registry.internal:8081/apis/ccompat/v7"),
            "https://registry.internal:8081/apis/ccompat/v7",
        );
    }

    #[test]
    fn a_descriptor_that_names_neither_a_group_nor_a_client_says_nothing() {
        assert!(operation(None, &[]).is_empty());
    }

    #[test]
    fn the_last_client_id_wins_as_it_does_in_the_consumer_config() {
        let config = [
            ("client.id".to_owned(), "first".to_owned()),
            ("client.id".to_owned(), "last".to_owned()),
        ];
        let json = serde_json::to_string(&operation(Some("orders-svc"), &config))
            .expect("bindings serialize");
        assert!(json.contains("last"), "got: {json}");
        assert!(!json.contains("first"), "got: {json}");
    }
}
