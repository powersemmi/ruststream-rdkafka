//! The registry's administrative surface: what a migration tool asks besides the four questions
//! the codecs ask.
//!
//! [`RegistryAdmin`] lists a subject's versions, fetches one, looks a schema up by its content,
//! asks the registry whether a schema is compatible (with the registry's own account of what
//! differs), registers a schema with its references, and reads and sets a subject's
//! compatibility level. It is what `cargo kafka-schema` drives; a deploy tool of a service's own
//! uses it the same way.
//!
//! It is a type of its own rather than more methods on
//! [`RegistryClient`](super::RegistryClient): that trait is the seam a service implements to
//! reach a registry from the codecs, and an administrative question a codec never asks does not
//! belong in what every implementation has to answer.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::client::{Auth, HttpConfig};
use super::{SchemaRegistry, SchemaType};
use crate::error::KafkaError;

/// The compatibility level a registry holds a subject to.
///
/// A `_TRANSITIVE` level checks a new schema against every version of the subject; the plain
/// level checks it against the latest one only.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::schema_registry::admin::CompatibilityLevel;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // What `subject.toml` says, parsed the way the registry spells it.
///     let level: CompatibilityLevel = "BACKWARD_TRANSITIVE".parse()?;
///     println!("new schemas must read every version: {}", level.checks_backward());
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CompatibilityLevel {
    /// No check.
    None,
    /// A reader of the new schema reads data written with the latest version.
    Backward,
    /// A reader of the new schema reads data written with every version.
    BackwardTransitive,
    /// A reader of the latest version reads data written with the new schema.
    Forward,
    /// A reader of every version reads data written with the new schema.
    ForwardTransitive,
    /// Both [`Backward`](Self::Backward) and [`Forward`](Self::Forward).
    Full,
    /// Both [`BackwardTransitive`](Self::BackwardTransitive) and
    /// [`ForwardTransitive`](Self::ForwardTransitive).
    FullTransitive,
}

impl CompatibilityLevel {
    /// The name the registry's API uses (`BACKWARD_TRANSITIVE`, ..).
    #[must_use]
    pub const fn as_api(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
        }
    }

    /// Whether a reader of the new schema must read what the older versions wrote.
    #[must_use]
    pub const fn checks_backward(self) -> bool {
        matches!(
            self,
            Self::Backward | Self::BackwardTransitive | Self::Full | Self::FullTransitive
        )
    }

    /// Whether a reader of the older versions must read what the new schema writes.
    #[must_use]
    pub const fn checks_forward(self) -> bool {
        matches!(
            self,
            Self::Forward | Self::ForwardTransitive | Self::Full | Self::FullTransitive
        )
    }

    /// Whether the check covers every version rather than the latest one.
    #[must_use]
    pub const fn is_transitive(self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }
}

impl fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_api())
    }
}

/// A compatibility level name the registry does not know.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "`{0}` is not a compatibility level; the registry knows NONE, BACKWARD, \
     BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL and FULL_TRANSITIVE"
)]
pub struct ParseCompatibilityLevelError(String);

impl FromStr for CompatibilityLevel {
    type Err = ParseCompatibilityLevelError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Ok(match name {
            "NONE" => Self::None,
            "BACKWARD" => Self::Backward,
            "BACKWARD_TRANSITIVE" => Self::BackwardTransitive,
            "FORWARD" => Self::Forward,
            "FORWARD_TRANSITIVE" => Self::ForwardTransitive,
            "FULL" => Self::Full,
            "FULL_TRANSITIVE" => Self::FullTransitive,
            other => return Err(ParseCompatibilityLevelError(other.to_owned())),
        })
    }
}

/// A schema another schema refers to: a Protobuf import, a JSON Schema `$ref`, a member of an
/// Avro union, held by a subject of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaReference {
    /// The name the referring schema uses: the import path, the `$ref`, the record's full name.
    pub name: String,
    /// The subject that holds the referenced schema.
    pub subject: String,
    /// The version of that subject the reference resolves to.
    pub version: u32,
}

/// A schema as a tool submits it: its flavor, its text, and what it refers to.
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::SchemaRegistry;
/// use ruststream_rdkafka::schema_registry::SchemaType;
/// use ruststream_rdkafka::schema_registry::admin::{SchemaDraft, SchemaReference};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let admin = SchemaRegistry::new("http://localhost:8081")
///         .admin()
///         .ok_or("the shipped HTTP client administers the registry")?;
///     // `payments.proto` imports `money.proto`, which subject `money-value` holds at version 3.
///     let draft = SchemaDraft::new(SchemaType::Protobuf, std::fs::read_to_string("payments.proto")?)
///         .reference(SchemaReference {
///             name: "money.proto".to_owned(),
///             subject: "money-value".to_owned(),
///             version: 3,
///         });
///     let id = admin.register("payments-value", &draft).await?;
///     println!("payments-value registered as schema {id}");
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDraft {
    schema_type: SchemaType,
    definition: String,
    references: Vec<SchemaReference>,
}

impl SchemaDraft {
    /// A schema of `schema_type` with no references.
    #[must_use]
    pub fn new(schema_type: SchemaType, definition: impl Into<String>) -> Self {
        Self {
            schema_type,
            definition: definition.into(),
            references: Vec::new(),
        }
    }

    /// Adds one reference.
    #[must_use]
    pub fn reference(mut self, reference: SchemaReference) -> Self {
        self.references.push(reference);
        self
    }

    /// The schema flavor.
    #[must_use]
    pub fn schema_type(&self) -> SchemaType {
        self.schema_type
    }

    /// The schema text.
    #[must_use]
    pub fn definition(&self) -> &str {
        &self.definition
    }

    /// The schemas this one refers to.
    #[must_use]
    pub fn references(&self) -> &[SchemaReference] {
        &self.references
    }

    fn body(&self) -> serde_json::Value {
        let mut body = json!({
            "schema": self.definition,
            "schemaType": self.schema_type.as_api(),
        });
        if !self.references.is_empty() {
            body["references"] = json!(self.references);
        }
        body
    }
}

/// One version of a subject, as the registry holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectVersion {
    subject: String,
    version: u32,
    id: u32,
    schema_type: SchemaType,
    definition: String,
    references: Vec<SchemaReference>,
}

impl SubjectVersion {
    /// The subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The version number within the subject. Versions start at 1 and may leave gaps where a
    /// version was deleted.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The registry-wide schema id: what the wire format carries.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The schema flavor.
    #[must_use]
    pub fn schema_type(&self) -> SchemaType {
        self.schema_type
    }

    /// The schema text as the registry stores it.
    #[must_use]
    pub fn definition(&self) -> &str {
        &self.definition
    }

    /// The schemas this version refers to.
    #[must_use]
    pub fn references(&self) -> &[SchemaReference] {
        &self.references
    }
}

/// The registry's answer to a compatibility question, with its own account of what differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityVerdict {
    compatible: bool,
    messages: Vec<String>,
}

impl CompatibilityVerdict {
    /// Whether the registry would register the schema.
    #[must_use]
    pub fn is_compatible(&self) -> bool {
        self.compatible
    }

    /// The registry's messages, one per difference: the error kind, the path, the field and the
    /// version it was compared with. Empty for a compatible schema.
    #[must_use]
    pub fn messages(&self) -> &[String] {
        &self.messages
    }
}

#[derive(Deserialize)]
struct VersionResponse {
    subject: String,
    version: u32,
    id: u32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<SchemaReference>,
}

impl From<VersionResponse> for SubjectVersion {
    fn from(response: VersionResponse) -> Self {
        Self {
            subject: response.subject,
            version: response.version,
            id: response.id,
            schema_type: SchemaType::from_api(response.schema_type.as_deref()),
            definition: response.schema,
            references: response.references,
        }
    }
}

#[derive(Deserialize)]
struct RegisterResponse {
    id: u32,
}

#[derive(Deserialize)]
struct CompatibilityResponse {
    is_compatible: bool,
    #[serde(default)]
    messages: Vec<String>,
}

#[derive(Deserialize)]
struct ConfigResponse {
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: String,
}

#[derive(Deserialize)]
struct RegistryErrorBody {
    error_code: Option<i64>,
    message: Option<String>,
}

/// The registry's administrative surface over the shipped HTTP client: versions, content
/// lookups, verbose compatibility checks, registration with references, and compatibility
/// levels.
///
/// Built from a [`SchemaRegistry`] with [`SchemaRegistry::admin`], so it speaks to the same
/// URL with the same credentials and request deadline.
///
/// # Examples
///
/// A deploy step that registers a schema only when the registry would accept it:
///
/// ```no_run
/// use ruststream_rdkafka::SchemaRegistry;
/// use ruststream_rdkafka::schema_registry::SchemaType;
/// use ruststream_rdkafka::schema_registry::admin::{CompatibilityLevel, SchemaDraft};
///
/// const ORDER_V2: &str = r#"{"type": "record", "name": "Order", "namespace": "com.acme.orders",
///     "fields": [{"name": "id", "type": "long"}, {"name": "note", "type": ["null", "string"],
///     "default": null}]}"#;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let admin = SchemaRegistry::new("http://localhost:8081")
///         .basic_auth("deployer", "secret")
///         .admin()
///         .ok_or("the shipped HTTP client administers the registry")?;
///     admin
///         .set_compatibility("orders-value", CompatibilityLevel::BackwardTransitive)
///         .await?;
///     let draft = SchemaDraft::new(SchemaType::Avro, ORDER_V2);
///     if let Some(applied) = admin.find("orders-value", &draft).await? {
///         println!("already version {} (id {})", applied.version(), applied.id());
///         return Ok(());
///     }
///     let verdict = admin.check_compatibility("orders-value", &draft).await?;
///     for message in verdict.messages() {
///         eprintln!("{message}");
///     }
///     if verdict.is_compatible() {
///         let id = admin.register("orders-value", &draft).await?;
///         println!("registered as schema {id}");
///     }
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct RegistryAdmin {
    config: HttpConfig,
    http: reqwest::Client,
}

impl fmt::Debug for RegistryAdmin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryAdmin")
            .field("base_url", &self.config.base_url)
            .field("request_timeout", &self.config.timeout)
            .finish_non_exhaustive()
    }
}

impl SchemaRegistry {
    /// The administrative surface of the registry this client speaks to: see [`RegistryAdmin`].
    ///
    /// `None` when this client was built with [`with_client`](Self::with_client): the
    /// administrative calls speak the registry's HTTP API with the shipped client's URL,
    /// credentials and deadline, which a caller-supplied client does not expose.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream_rdkafka::SchemaRegistry;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let admin = SchemaRegistry::new("http://localhost:8081")
    ///         .admin()
    ///         .ok_or("the shipped HTTP client administers the registry")?;
    ///     println!("orders-value holds versions {:?}", admin.versions("orders-value").await?);
    ///     Ok(())
    /// }
    /// ```
    #[must_use]
    pub fn admin(&self) -> Option<RegistryAdmin> {
        let config = self.inner.shipped.clone()?;
        Some(RegistryAdmin {
            config,
            http: reqwest::Client::new(),
        })
    }
}

impl RegistryAdmin {
    /// The versions `subject` holds, oldest first; empty when the registry has no such subject.
    ///
    /// Soft-deleted versions are left out, so the list may have gaps.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request, and [`KafkaError::SchemaRegistryTimeout`] when it does not answer in time.
    pub async fn versions(&self, subject: &str) -> Result<Vec<u32>, KafkaError> {
        let path = format!("/subjects/{subject}/versions");
        let response = self
            .send(self.request(reqwest::Method::GET, &path), &path)
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let mut versions: Vec<u32> = self.read(response, &path).await?;
        versions.sort_unstable();
        Ok(versions)
    }

    /// Version `version` of `subject`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request or holds no such version, and [`KafkaError::SchemaRegistryTimeout`] when it does
    /// not answer in time.
    pub async fn version(&self, subject: &str, version: u32) -> Result<SubjectVersion, KafkaError> {
        let path = format!("/subjects/{subject}/versions/{version}");
        let response = self
            .send(self.request(reqwest::Method::GET, &path), &path)
            .await?;
        let found: VersionResponse = self.read(response, &path).await?;
        Ok(found.into())
    }

    /// The version of `subject` whose schema is `draft`, or `None` when the subject holds no
    /// such schema (or does not exist).
    ///
    /// The registry compares the schemas itself, after parsing them, so formatting does not
    /// matter. This is how a migration tool tells an applied migration from a pending one.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request, and [`KafkaError::SchemaRegistryTimeout`] when it does not answer in time.
    pub async fn find(
        &self,
        subject: &str,
        draft: &SchemaDraft,
    ) -> Result<Option<SubjectVersion>, KafkaError> {
        let path = format!("/subjects/{subject}");
        let request = self
            .request(reqwest::Method::POST, &path)
            .json(&draft.body());
        let response = self.send(request, &path).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let found: VersionResponse = self.read(response, &path).await?;
        Ok(Some(found.into()))
    }

    /// Whether the registry would register `draft` under `subject`, with its messages.
    ///
    /// The check runs against every version the subject's level names: the latest one for a
    /// plain level, all of them for a `_TRANSITIVE` one. A subject with no versions accepts
    /// any schema.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request (a schema that does not parse included), and
    /// [`KafkaError::SchemaRegistryTimeout`] when it does not answer in time.
    pub async fn check_compatibility(
        &self,
        subject: &str,
        draft: &SchemaDraft,
    ) -> Result<CompatibilityVerdict, KafkaError> {
        // `/versions` without a number is the check registration itself runs; `/versions/latest`
        // compares with the latest version alone, even under a `_TRANSITIVE` level.
        let path = format!("/compatibility/subjects/{subject}/versions?verbose=true");
        let request = self
            .request(reqwest::Method::POST, &path)
            .json(&draft.body());
        let response = self.send(request, &path).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(CompatibilityVerdict {
                compatible: true,
                messages: Vec::new(),
            });
        }
        let verdict: CompatibilityResponse = self.read(response, &path).await?;
        Ok(CompatibilityVerdict {
            compatible: verdict.is_compatible,
            messages: verdict.messages,
        })
    }

    /// Registers `draft` under `subject` and returns its schema id. A schema the subject already
    /// holds keeps its version and id.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request, refuses the schema as incompatible or cannot resolve a reference, and
    /// [`KafkaError::SchemaRegistryTimeout`] when it does not answer in time.
    pub async fn register(&self, subject: &str, draft: &SchemaDraft) -> Result<u32, KafkaError> {
        let path = format!("/subjects/{subject}/versions");
        let request = self
            .request(reqwest::Method::POST, &path)
            .json(&draft.body());
        let response = self.send(request, &path).await?;
        let registered: RegisterResponse = self.read(response, &path).await?;
        Ok(registered.id)
    }

    /// The compatibility level set on `subject` itself, or `None` when the subject follows the
    /// registry's global level.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request or answers with a level this crate does not know, and
    /// [`KafkaError::SchemaRegistryTimeout`] when it does not answer in time.
    pub async fn compatibility(
        &self,
        subject: &str,
    ) -> Result<Option<CompatibilityLevel>, KafkaError> {
        let path = format!("/config/{subject}?defaultToGlobal=false");
        let response = self
            .send(self.request(reqwest::Method::GET, &path), &path)
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let config: ConfigResponse = self.read(response, &path).await?;
        config
            .compatibility_level
            .parse()
            .map(Some)
            .map_err(KafkaError::schema_registry)
    }

    /// Sets `subject`'s compatibility level. A subject with no versions yet takes it too, so the
    /// level holds from its first registration.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request, and [`KafkaError::SchemaRegistryTimeout`] when it does not answer in time.
    pub async fn set_compatibility(
        &self,
        subject: &str,
        level: CompatibilityLevel,
    ) -> Result<(), KafkaError> {
        let path = format!("/config/{subject}");
        let request = self
            .request(reqwest::Method::PUT, &path)
            .json(&json!({ "compatibility": level.as_api() }));
        let response = self.send(request, &path).await?;
        let _: serde_json::Value = self.read(response, &path).await?;
        Ok(())
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.config.base_url);
        let request = self
            .http
            .request(method, url)
            .timeout(self.config.timeout)
            .header(
                reqwest::header::ACCEPT,
                "application/vnd.schemaregistry.v1+json",
            );
        match &self.config.auth {
            Auth::None => request,
            Auth::Basic { user, password } => request.basic_auth(user, Some(password)),
            Auth::Bearer(token) => request.bearer_auth(token),
        }
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        path: &str,
    ) -> Result<reqwest::Response, KafkaError> {
        request.send().await.map_err(|err| self.failed(path, err))
    }

    /// Reads a successful answer, or turns a failing one into an error that carries the
    /// registry's own code and message: the difference between "422" and "the schema does not
    /// parse at line 3".
    async fn read<Answer: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
        path: &str,
    ) -> Result<Answer, KafkaError> {
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let detail = match serde_json::from_str::<RegistryErrorBody>(&body) {
                Ok(RegistryErrorBody {
                    error_code: Some(code),
                    message: Some(message),
                }) => format!("error {code}: {message}"),
                _ => body,
            };
            return Err(KafkaError::SchemaRegistry(
                format!("the registry answered {path} with {status}: {detail}").into(),
            ));
        }
        response.json().await.map_err(|err| self.failed(path, err))
    }

    fn failed(&self, path: &str, err: reqwest::Error) -> KafkaError {
        if err.is_timeout() {
            return KafkaError::SchemaRegistryTimeout {
                request: path.to_owned(),
                timeout: self.config.timeout,
            };
        }
        // Without the URL: the base URL may carry credentials in its user part.
        KafkaError::schema_registry(err.without_url())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::future::BoxFuture;
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::schema_registry::{RegisteredSchema, RegistryClient};

    const ORDER: &str =
        r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}"#;

    fn admin(server: &MockServer) -> RegistryAdmin {
        SchemaRegistry::new(server.uri())
            .admin()
            .expect("shipped client")
    }

    fn version_body(version: u32, id: u32) -> serde_json::Value {
        json!({
            "subject": "orders-value",
            "version": version,
            "id": id,
            "schema": ORDER,
        })
    }

    #[test]
    fn levels_parse_from_their_api_names_and_back() {
        for level in [
            CompatibilityLevel::None,
            CompatibilityLevel::Backward,
            CompatibilityLevel::BackwardTransitive,
            CompatibilityLevel::Forward,
            CompatibilityLevel::ForwardTransitive,
            CompatibilityLevel::Full,
            CompatibilityLevel::FullTransitive,
        ] {
            assert_eq!(level.as_api().parse::<CompatibilityLevel>(), Ok(level));
        }
        let err = "backward"
            .parse::<CompatibilityLevel>()
            .expect_err("lower case");
        assert!(
            err.to_string()
                .contains("`backward` is not a compatibility level")
        );
    }

    #[test]
    fn levels_name_their_directions() {
        assert!(CompatibilityLevel::Backward.checks_backward());
        assert!(!CompatibilityLevel::Backward.checks_forward());
        assert!(!CompatibilityLevel::Backward.is_transitive());
        assert!(CompatibilityLevel::ForwardTransitive.checks_forward());
        assert!(!CompatibilityLevel::ForwardTransitive.checks_backward());
        assert!(CompatibilityLevel::FullTransitive.checks_backward());
        assert!(CompatibilityLevel::FullTransitive.checks_forward());
        assert!(CompatibilityLevel::FullTransitive.is_transitive());
        assert!(!CompatibilityLevel::None.checks_backward());
        assert!(!CompatibilityLevel::None.checks_forward());
    }

    struct Foreign;

    impl RegistryClient for Foreign {
        fn schema_by_id(
            &self,
            id: u32,
        ) -> BoxFuture<'_, Result<Arc<RegisteredSchema>, KafkaError>> {
            Box::pin(
                async move { Ok(Arc::new(RegisteredSchema::new(id, SchemaType::Avro, ORDER))) },
            )
        }

        fn latest(
            &self,
            _subject: &str,
        ) -> BoxFuture<'_, Result<Option<Arc<RegisteredSchema>>, KafkaError>> {
            Box::pin(async { Ok(None) })
        }

        fn register(
            &self,
            _subject: &str,
            _schema_type: SchemaType,
            _definition: String,
        ) -> BoxFuture<'_, Result<u32, KafkaError>> {
            Box::pin(async { Ok(1) })
        }

        fn lookup_id(
            &self,
            _subject: &str,
            _schema_type: SchemaType,
            _definition: String,
        ) -> BoxFuture<'_, Result<u32, KafkaError>> {
            Box::pin(async { Ok(1) })
        }
    }

    #[test]
    fn a_caller_supplied_client_has_no_admin() {
        assert!(
            SchemaRegistry::with_client(Arc::new(Foreign))
                .admin()
                .is_none()
        );
    }

    #[tokio::test]
    async fn versions_come_sorted_and_a_missing_subject_has_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([3, 1, 2])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/gone-value/versions"))
            .respond_with(ResponseTemplate::new(404).set_body_json(
                json!({"error_code": 40401, "message": "Subject 'gone-value' not found."}),
            ))
            .mount(&server)
            .await;

        let admin = admin(&server);
        assert_eq!(
            admin.versions("orders-value").await.expect("versions"),
            [1, 2, 3]
        );
        let missing = admin.versions("gone-value").await.expect("missing");
        assert_eq!(missing, Vec::<u32>::new());
    }

    #[tokio::test]
    async fn one_version_reads_its_schema_and_references() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/payments-value/versions/2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "subject": "payments-value",
                "version": 2,
                "id": 41,
                "schemaType": "PROTOBUF",
                "schema": "syntax = \"proto3\";",
                "references": [{"name": "money.proto", "subject": "money-value", "version": 1}],
            })))
            .mount(&server)
            .await;

        let found = admin(&server)
            .version("payments-value", 2)
            .await
            .expect("version");
        assert_eq!(found.subject(), "payments-value");
        assert_eq!((found.version(), found.id()), (2, 41));
        assert_eq!(found.schema_type(), SchemaType::Protobuf);
        assert_eq!(found.definition(), "syntax = \"proto3\";");
        assert_eq!(
            found.references(),
            [SchemaReference {
                name: "money.proto".to_owned(),
                subject: "money-value".to_owned(),
                version: 1,
            }]
        );
    }

    #[tokio::test]
    async fn find_posts_the_content_and_reads_a_miss_as_none() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value"))
            .and(body_json(json!({"schema": ORDER, "schemaType": "AVRO"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(version_body(2, 7)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/subjects/other-value"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error_code": 40403, "message": "Schema not found"})),
            )
            .mount(&server)
            .await;

        let admin = admin(&server);
        let draft = SchemaDraft::new(SchemaType::Avro, ORDER);
        let found = admin
            .find("orders-value", &draft)
            .await
            .expect("find")
            .expect("applied");
        assert_eq!((found.version(), found.id()), (2, 7));
        assert!(
            admin
                .find("other-value", &draft)
                .await
                .expect("miss")
                .is_none()
        );
    }

    #[tokio::test]
    async fn the_compatibility_check_asks_every_version_verbosely() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/compatibility/subjects/orders-value/versions"))
            .and(query_param("verbose", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "is_compatible": false,
                "messages": ["{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', description:'..', \
                              additionalInfo:'qty', version: 1}"],
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/compatibility/subjects/new-value/versions"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let admin = admin(&server);
        let draft = SchemaDraft::new(SchemaType::Avro, ORDER);
        let verdict = admin
            .check_compatibility("orders-value", &draft)
            .await
            .expect("check");
        assert!(!verdict.is_compatible());
        assert_eq!(verdict.messages().len(), 1);
        assert!(verdict.messages()[0].contains("READER_FIELD_MISSING_DEFAULT_VALUE"));
        let fresh = admin
            .check_compatibility("new-value", &draft)
            .await
            .expect("fresh");
        assert!(fresh.is_compatible() && fresh.messages().is_empty());
    }

    #[tokio::test]
    async fn register_sends_the_references() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/payments-value/versions"))
            .and(body_json(json!({
                "schema": "import \"money.proto\";",
                "schemaType": "PROTOBUF",
                "references": [{"name": "money.proto", "subject": "money-value", "version": 3}],
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 55})))
            .mount(&server)
            .await;

        let draft = SchemaDraft::new(SchemaType::Protobuf, "import \"money.proto\";").reference(
            SchemaReference {
                name: "money.proto".to_owned(),
                subject: "money-value".to_owned(),
                version: 3,
            },
        );
        let id = admin(&server)
            .register("payments-value", &draft)
            .await
            .expect("register");
        assert_eq!(id, 55);
    }

    #[tokio::test]
    async fn a_refusal_carries_the_registry_code_and_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(422).set_body_json(json!({
                "error_code": 42201,
                "message": "Either the input schema or one of its references is invalid",
            })))
            .mount(&server)
            .await;

        let draft = SchemaDraft::new(SchemaType::Avro, "{");
        let err = admin(&server)
            .register("orders-value", &draft)
            .await
            .expect_err("invalid");
        let text = err.to_string();
        assert!(text.contains("422"), "{text}");
        assert!(
            text.contains("error 42201: Either the input schema"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn levels_read_and_set_per_subject() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/config/orders-value"))
            .and(query_param("defaultToGlobal", "false"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"compatibilityLevel": "FULL_TRANSITIVE"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/config/plain-value"))
            .respond_with(ResponseTemplate::new(404).set_body_json(
                json!({"error_code": 40408, "message": "no subject-level compatibility"}),
            ))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/config/orders-value"))
            .and(body_json(json!({"compatibility": "BACKWARD_TRANSITIVE"})))
            .and(header("accept", "application/vnd.schemaregistry.v1+json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"compatibility": "BACKWARD_TRANSITIVE"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let admin = admin(&server);
        assert_eq!(
            admin.compatibility("orders-value").await.expect("level"),
            Some(CompatibilityLevel::FullTransitive)
        );
        assert_eq!(
            admin.compatibility("plain-value").await.expect("unset"),
            None
        );
        admin
            .set_compatibility("orders-value", CompatibilityLevel::BackwardTransitive)
            .await
            .expect("set");
    }

    #[test]
    fn debug_leaves_the_credentials_out() {
        let admin = SchemaRegistry::new("http://registry:8081")
            .basic_auth("svc", "hunter2")
            .admin()
            .expect("shipped client");
        let printed = format!("{admin:?}");
        assert!(printed.contains("http://registry:8081"));
        assert!(!printed.contains("hunter2"));
    }
}
