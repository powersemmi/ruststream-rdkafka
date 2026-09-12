//! What a registry answers: the seam, and the HTTP implementation this crate ships.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Deserialize;

use super::{RegisteredSchema, SchemaType};
use crate::error::KafkaError;

/// The deadline one registry request gets unless the client names another.
///
/// Ten seconds sits above anything a healthy registry needs, a cold one and a slow hop included,
/// and far below the stall an absent deadline allows on the delivery path.
pub(super) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The async half of a [`SchemaRegistry`](super::SchemaRegistry): the conversation with the
/// registry itself.
///
/// This is the seam a service reaches for when the client that ships here is not the one it
/// wants - a different HTTP stack, a published client crate, a binding to a non-Rust one, a fake
/// in a test. Everything above it (the codecs, the byte-lane subjects, the prefetch, the
/// transcoding middleware) is written against [`SchemaRegistry`](super::SchemaRegistry), which is a facade over this
/// trait and a [`SchemaCache`](super::SchemaCache), so an implementation here reaches all of it.
///
/// Every method is async and returns a boxed future, because the trait is used as
/// `Arc<dyn RegistryClient>`: a virtual call and one allocation per *registry round trip* is
/// nothing beside the round trip, and erasing the type here is what keeps the codecs from
/// growing a type parameter that would then have to be written out at every mount site.
///
/// # Examples
///
/// A client that answers from a fixed table, which is all a test usually needs:
///
/// ```
/// use std::sync::Arc;
///
/// use futures::future::BoxFuture;
/// use ruststream_rdkafka::KafkaError;
/// use ruststream_rdkafka::schema_registry::{RegisteredSchema, RegistryClient, SchemaType};
///
/// struct Fixed(Arc<RegisteredSchema>);
///
/// impl RegistryClient for Fixed {
///     fn schema_by_id(&self, _id: u32) -> BoxFuture<'_, Result<Arc<RegisteredSchema>, KafkaError>> {
///         let schema = Arc::clone(&self.0);
///         Box::pin(async move { Ok(schema) })
///     }
///
///     fn latest(
///         &self,
///         _subject: &str,
///     ) -> BoxFuture<'_, Result<Option<Arc<RegisteredSchema>>, KafkaError>> {
///         let schema = Arc::clone(&self.0);
///         Box::pin(async move { Ok(Some(schema)) })
///     }
///
///     fn register(
///         &self,
///         _subject: &str,
///         _schema_type: SchemaType,
///         _definition: String,
///     ) -> BoxFuture<'_, Result<u32, KafkaError>> {
///         let id = self.0.id();
///         Box::pin(async move { Ok(id) })
///     }
///
///     fn lookup_id(
///         &self,
///         subject: &str,
///         schema_type: SchemaType,
///         definition: String,
///     ) -> BoxFuture<'_, Result<u32, KafkaError>> {
///         self.register(subject, schema_type, definition)
///     }
/// }
/// # fn check() {
/// let _ = Fixed(Arc::new(RegisteredSchema::new(1, SchemaType::Avro, "\"string\"")));
/// # }
/// # check();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a schema registry client",
    note = "a client answers the four registry questions asynchronously: a schema by id, a \
            subject's latest version, registering a schema, and looking one up without \
            registering it"
)]
pub trait RegistryClient: Send + Sync + 'static {
    /// The schema the registry holds under `id`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request, or does not know the id.
    fn schema_by_id(&self, id: u32) -> BoxFuture<'_, Result<Arc<RegisteredSchema>, KafkaError>>;

    /// `subject`'s latest version, or `None` when the registry has no such subject.
    ///
    /// A missing subject is `None` rather than an error because the framing middleware reads it
    /// as "this topic is not registry-backed", which is an ordinary topology, not a failure.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request.
    fn latest(
        &self,
        subject: &str,
    ) -> BoxFuture<'_, Result<Option<Arc<RegisteredSchema>>, KafkaError>>;

    /// Registers `definition` under `subject` and returns the id, which is idempotent
    /// registry-side: an identical schema keeps the id it already had.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request, or refuses the schema as incompatible with the subject's history.
    fn register(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>>;

    /// The id `definition` already has under `subject`, registering nothing.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request, and [`KafkaError::InvalidOptions`] when the subject does not hold this schema.
    fn lookup_id(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>>;

    /// Whether `definition` is compatible with `subject`'s latest version, under whatever
    /// compatibility level the subject is configured with.
    ///
    /// This is what Confluent's `latest.compatibility.strict` checks. The default answers `None`,
    /// meaning "this client cannot tell", and the caller then skips the check rather than reading
    /// silence as either answer - so a client written before this method existed keeps working.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request.
    fn is_compatible(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: String,
    ) -> BoxFuture<'_, Result<Option<bool>, KafkaError>> {
        let _ = (subject, schema_type, definition);
        Box::pin(async { Ok(None) })
    }
}

// No `Debug`: the variants hold credentials, and the registry's own `Debug` leaves them out.
#[derive(Clone)]
pub(super) enum Auth {
    None,
    Basic { user: String, password: String },
    Bearer(String),
}

/// Everything the shipped HTTP client is configured with, kept whole so that changing one
/// setting rebuilds the client without dropping the others.
#[derive(Clone)]
pub(super) struct HttpConfig {
    pub(super) base_url: String,
    pub(super) auth: Auth,
    pub(super) timeout: Duration,
}

impl HttpConfig {
    pub(super) fn new(base_url: String) -> Self {
        Self {
            base_url,
            auth: Auth::None,
            timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

#[derive(Deserialize)]
struct RegisterResponse {
    id: u32,
}

#[derive(Deserialize)]
struct CompatibilityResponse {
    is_compatible: bool,
    /// Present with `?verbose=true`: the registry's own account of what differs.
    #[serde(default)]
    messages: Vec<String>,
}

#[derive(Deserialize)]
struct LatestVersionResponse {
    id: u32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

/// The [`RegistryClient`] this crate ships: Confluent's HTTP API over `reqwest`, rustls only.
pub struct HttpRegistryClient {
    config: HttpConfig,
    http: reqwest::Client,
}

impl fmt::Debug for HttpRegistryClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRegistryClient")
            .field("base_url", &self.config.base_url)
            .field("request_timeout", &self.config.timeout)
            .finish_non_exhaustive()
    }
}

impl HttpRegistryClient {
    pub(super) fn new(config: HttpConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.config.base_url);
        let request = self.http.request(method, url).timeout(self.config.timeout);
        match &self.config.auth {
            Auth::None => request,
            Auth::Basic { user, password } => request.basic_auth(user, Some(password)),
            Auth::Bearer(token) => request.bearer_auth(token),
        }
    }

    /// Turns one request failure into a crate error, telling a silent registry apart from a
    /// failing one: only the expiry of this client's deadline becomes the timeout error.
    fn failed(&self, path: &str, err: reqwest::Error) -> KafkaError {
        if err.is_timeout() {
            return KafkaError::SchemaRegistryTimeout {
                request: path.to_owned(),
                timeout: self.config.timeout,
            };
        }
        KafkaError::schema_registry(err)
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, KafkaError> {
        let response = self
            .request(reqwest::Method::GET, path)
            .send()
            .await
            .map_err(|err| self.failed(path, err))?
            .error_for_status()
            .map_err(KafkaError::schema_registry)?;
        response.json().await.map_err(|err| self.failed(path, err))
    }

    async fn post_schema(
        &self,
        path: String,
        schema_type: SchemaType,
        definition: String,
        missing: Option<String>,
    ) -> Result<u32, KafkaError> {
        let body = serde_json::json!({
            "schema": definition,
            "schemaType": schema_type.as_api(),
        });
        let response = self
            .request(reqwest::Method::POST, &path)
            .json(&body)
            .send()
            .await
            .map_err(|err| self.failed(&path, err))?;
        if let Some(message) = missing
            && response.status() == reqwest::StatusCode::NOT_FOUND
        {
            return Err(KafkaError::InvalidOptions(message));
        }
        let response = response
            .error_for_status()
            .map_err(KafkaError::schema_registry)?;
        let registered: RegisterResponse = response
            .json()
            .await
            .map_err(|err| self.failed(&path, err))?;
        Ok(registered.id)
    }
}

impl RegistryClient for HttpRegistryClient {
    fn schema_by_id(&self, id: u32) -> BoxFuture<'_, Result<Arc<RegisteredSchema>, KafkaError>> {
        Box::pin(async move {
            let fetched: SchemaByIdResponse = self.get_json(&format!("/schemas/ids/{id}")).await?;
            Ok(Arc::new(RegisteredSchema::new(
                id,
                SchemaType::from_api(fetched.schema_type.as_deref()),
                fetched.schema,
            )))
        })
    }

    fn latest(
        &self,
        subject: &str,
    ) -> BoxFuture<'_, Result<Option<Arc<RegisteredSchema>>, KafkaError>> {
        let path = format!("/subjects/{subject}/versions/latest");
        Box::pin(async move {
            let response = self
                .request(reqwest::Method::GET, &path)
                .send()
                .await
                .map_err(|err| self.failed(&path, err))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let response = response
                .error_for_status()
                .map_err(KafkaError::schema_registry)?;
            let fetched: LatestVersionResponse = response
                .json()
                .await
                .map_err(|err| self.failed(&path, err))?;
            Ok(Some(Arc::new(RegisteredSchema::new(
                fetched.id,
                SchemaType::from_api(fetched.schema_type.as_deref()),
                fetched.schema,
            ))))
        })
    }

    fn register(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>> {
        let path = format!("/subjects/{subject}/versions");
        Box::pin(self.post_schema(path, schema_type, definition, None))
    }

    fn lookup_id(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>> {
        let path = format!("/subjects/{subject}");
        let missing = format!(
            "the registry holds no such schema under subject {subject:?}; register it there \
             first (the typed shorthands do it in one call) or point the producer at the \
             subject that carries this schema",
        );
        Box::pin(self.post_schema(path, schema_type, definition, Some(missing)))
    }

    fn is_compatible(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: String,
    ) -> BoxFuture<'_, Result<Option<bool>, KafkaError>> {
        let path = format!("/compatibility/subjects/{subject}/versions/latest?verbose=true");
        let subject = subject.to_owned();
        Box::pin(async move {
            let body = serde_json::json!({
                "schema": definition,
                "schemaType": schema_type.as_api(),
            });
            let response = self
                .request(reqwest::Method::POST, &path)
                .json(&body)
                .send()
                .await
                .map_err(|err| self.failed(&path, err))?;
            // A subject with no versions cannot be checked against one; that is the missing
            // subject case, which the caller has already settled by its own policy.
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let response = response
                .error_for_status()
                .map_err(KafkaError::schema_registry)?;
            let verdict: CompatibilityResponse = response
                .json()
                .await
                .map_err(|err| self.failed(&path, err))?;
            if !verdict.is_compatible {
                // The registry names the offending field; passing it through is the difference
                // between a usable startup failure and "incompatible".
                return Err(KafkaError::SchemaRegistry(
                    format!(
                        "the schema a codec publishes under subject {subject:?} is not \
                         compatible with the version the registry holds: {}",
                        verdict.messages.join(" "),
                    )
                    .into(),
                ));
            }
            Ok(Some(true))
        })
    }
}
