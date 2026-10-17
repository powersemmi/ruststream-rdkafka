//! The librdkafka properties a service passes through, kept as it set them and printed without
//! its credentials.

use std::fmt;
use std::ops::Deref;
use std::slice::Iter;

/// The properties librdkafka marks sensitive (`_RK_SENSITIVE` in its configuration table, which
/// it redacts from its own debug dump) and the aliases that set them. librdkafka offers no call
/// that answers this for a key, so the list follows the bundled librdkafka's table.
const SENSITIVE: [&str; 15] = [
    "sasl.username",
    "sasl.password",
    "sasl.oauthbearer.config",
    "sasl.oauthbearer.client.secret",
    "sasl.oauthbearer.client.credentials.client.secret",
    "sasl.oauthbearer.assertion.private.key.file",
    "sasl.oauthbearer.assertion.private.key.passphrase",
    "sasl.oauthbearer.assertion.private.key.pem",
    "ssl.key.location",
    "ssl.key.password",
    "ssl.key.pem",
    "ssl_key",
    "ssl.ca.pem",
    "ssl.keystore.password",
    // Not a librdkafka property, but the Java client's name for a store password, which a
    // configuration copied from one carries.
    "ssl.truststore.password",
];

/// Raw librdkafka properties in the order they were set; a later setting of a key wins, as it
/// does in librdkafka.
#[derive(Clone, Default)]
pub(crate) struct Properties(Vec<(String, String)>);

impl Properties {
    pub(crate) fn set(&mut self, key: String, value: String) {
        self.0.push((key, value));
    }
}

impl Deref for Properties {
    type Target = [(String, String)];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for &'a Properties {
    type Item = &'a (String, String);
    type IntoIter = Iter<'a, (String, String)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// What a sensitive property prints in place of its value.
struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

// A broker or descriptor is printed into logs, and a credential that reaches a log has left the
// service.
impl fmt::Debug for Properties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for (key, value) in &self.0 {
            if SENSITIVE.contains(&key.as_str()) {
                list.entry(&(key, Redacted));
            } else {
                list.entry(&(key, value));
            }
        }
        list.finish()
    }
}
