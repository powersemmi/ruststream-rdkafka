//! Avro as a codec: the schema lives in the codec, and handlers stay ordinary functions over
//! ordinary structs. Each delivery is read with the writer schema its envelope names, and each
//! reply is framed with the id of its type's subject.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_avro_codec --features avro -- run
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_avro_codec --features avro,testing
//! ```

use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::{SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
struct Delivery {
    id: i64,
    address: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, Outgoing)]
#[outgoing(name = "delivery-receipts")]
struct Receipt {
    id: i64,
    delivered: bool,
}

#[subscriber("deliveries", reply)]
async fn deliver(delivery: &Delivery) -> Receipt {
    tracing::info!(delivery.id, delivery.address, "delivered");
    Receipt {
        id: delivery.id,
        delivered: true,
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    // The prefetch resolves subjects at connect and each writer schema before its decode.
    let prefetch = SchemaPrefetch::new(registry);
    // Registration is the publish side: the subject a type's values are framed under.
    let codec = AvroCodec::registry(&prefetch).register::<Receipt>("delivery-receipts-value");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("deliveries-svc")
        .schema_prefetch(prefetch);

    // The scope's codec decodes every delivery; the reply names it again, at its own position.
    let replies = codec.clone();
    RustStream::new(AppInfo::new("deliveries", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(deliver)
            .out_reply(Publish::default())
            .codec(replies);
    })
}

#[ruststream::app]
fn service() -> impl App {
    app(SchemaRegistry::new("http://localhost:8081"))
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::codec::Codec;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::avro::{self, AvroCodec};
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::parse_envelope;
    use ruststream_rdkafka::{SchemaPrefetch, SchemaRegistry};

    use super::{Delivery, Receipt, app, live};

    #[tokio::test]
    async fn an_avro_delivery_is_answered_with_an_avro_receipt() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        // The producers own both subjects; this service registers neither.
        let registry = SchemaRegistry::new(&url);
        let deliveries = registry
            .register_avro::<Delivery>("deliveries-value")
            .await?;
        let receipts = registry
            .register_avro::<Receipt>("delivery-receipts-value")
            .await?;
        let writer = AvroCodec::registry(&SchemaPrefetch::new(registry.clone()))
            .register::<Delivery>("deliveries-value");
        let framed = writer.encode(&Delivery {
            id: 42,
            address: "Baker Street 221B".to_owned(),
        })?;
        assert_eq!(parse_envelope(&framed).map(|(id, _)| id), Some(deliveries));
        let tb = TestApp::start(app(registry)).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(framed.to_vec()))
            .to("deliveries")
            .publish()
            .await?;

        let published = kafka
            .published::<()>("delivery-receipts")
            .assert_called_once();
        let (id, datum) = parse_envelope(published.messages()[0].payload())
            .ok_or("the receipt went out without an envelope")?;
        assert_eq!(id, receipts);
        assert_eq!(
            avro::decode::<Receipt>(datum)?,
            Receipt {
                id: 42,
                delivered: true,
            },
        );
        Ok(())
    }

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);
}
