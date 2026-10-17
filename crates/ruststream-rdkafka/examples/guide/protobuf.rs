//! The shipping service of the Protobuf guide: parcels arrive as Confluent-framed Protobuf, and
//! each label leaves framed under the schema id of its subject.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_protobuf --features protobuf,testing -- run
//! ```

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::protobuf::decode_confluent;
use ruststream_rdkafka::{KafkaPublish, SchemaRegistry};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

// --8<-- [start:messages]
/// What `prost-build` generates for `shipping.Parcel`, plus the decode lane.
#[derive(Clone, PartialEq, prost::Message, Deserialized)]
#[wire(decode = decode_confluent)]
struct Parcel {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    address: String,
}

/// What `prost-build` generates for `shipping.Label`, plus the encode lane and its topic.
#[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
#[wire(encode = ::prost::Message::encode)]
#[outgoing(name = "labels")]
struct Label {
    #[prost(int64, tag = "1")]
    parcel: i64,
    #[prost(string, tag = "2")]
    address: String,
}
// --8<-- [end:messages]

// --8<-- [start:handler]
#[subscriber("parcels", reply)]
async fn print_label(parcel: &Parcel) -> Result<Label, HandlerOutcome> {
    if parcel.address.is_empty() {
        tracing::warn!(parcel.id, "parcel without an address dropped");
        return Err(HandlerOutcome::drop());
    }
    tracing::info!(parcel.id, "label printed");
    Ok(Label {
        parcel: parcel.id,
        address: parcel.address.clone(),
    })
}
// --8<-- [end:handler]

// --8<-- [start:service]
fn service(broker: KafkaBroker, registry: SchemaRegistry) -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        b.include(print_label)
            .out_reply(KafkaPublish::framed(&registry));
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(
        KafkaBroker::new(["localhost:9092"]).default_group("shipping-svc"),
        SchemaRegistry::new("http://localhost:8081"),
    )
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::protobuf::decode_confluent;
    use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};

    use super::{Label, Parcel, app, live, service};

    /// The schema the labels subject holds, registered by whoever owns the topic.
    const LABEL_PROTO: &str = r#"
syntax = "proto3";
package shipping;

message Label {
  int64 parcel = 1;
  string address = 2;
}
"#;

    /// A record as a registry-backed producer writes it, published as it is.
    #[derive(Serialized, Outgoing)]
    struct Record(Vec<u8>);

    /// `parcel` in the Confluent envelope: schema id 42, then the message-index path of the
    /// schema's first message, then the message.
    fn framed(parcel: &Parcel) -> Record {
        let mut indexed = vec![0x00];
        prost::Message::encode(parcel, &mut indexed).expect("a Vec grows to fit");
        Record(encode_envelope(42, &indexed))
    }

    #[tokio::test]
    async fn a_parcel_without_an_address_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<KafkaBroker>()
            .message(&framed(&Parcel {
                id: 7,
                address: String::new(),
            }))
            .to("parcels")
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("parcels")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<KafkaBroker>()
            .published::<()>("labels")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    /// The stand's registry, inside a context of this test's own, so the subject it registers is
    /// seen by no other run.
    fn registry(test: &str) -> Option<SchemaRegistry> {
        let url = live::url("SCHEMA_REGISTRY_TEST_URL")?;
        Some(SchemaRegistry::new(format!(
            "{url}/contexts/.guide-protobuf-{}-{test}",
            process::id(),
        )))
    }

    fn broker() -> KafkaBroker {
        KafkaBroker::new(["localhost:9092"]).default_group("shipping-svc")
    }

    #[tokio::test]
    async fn a_label_leaves_framed_under_its_subject() -> Result<(), Box<dyn Error>> {
        let Some(registry) = registry("label") else {
            return Ok(());
        };
        let id = registry
            .register("labels-value", SchemaType::Protobuf, LABEL_PROTO)
            .await?;
        let tb = TestApp::start(service(broker(), registry)).await?;

        tb.broker::<KafkaBroker>()
            .message(&framed(&Parcel {
                id: 8,
                address: "1 Harbour Road".to_owned(),
            }))
            .to("parcels")
            .publish()
            .await?;

        let published = tb
            .broker::<KafkaBroker>()
            .published::<()>("labels")
            .assert_called_once();
        let payload = published.messages()[0].payload();
        assert_eq!(
            parse_envelope(payload).map(|(wire_id, _)| wire_id),
            Some(id)
        );
        assert_eq!(
            decode_confluent::<Label>(payload)?,
            Label {
                parcel: 8,
                address: "1 Harbour Road".to_owned(),
            },
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_label_without_its_subject_is_not_published() -> Result<(), Box<dyn Error>> {
        let Some(registry) = registry("unregistered") else {
            return Ok(());
        };
        let tb = TestApp::start(service(broker(), registry)).await?;

        tb.broker::<KafkaBroker>()
            .message(&framed(&Parcel {
                id: 9,
                address: "1 Harbour Road".to_owned(),
            }))
            .to("parcels")
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("parcels")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker::<KafkaBroker>()
            .published::<()>("labels")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
