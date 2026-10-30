//! The text of a mismatch, written at compile time: a fixed buffer the comparison fills, and the
//! path from the checked type down to the place that differs.

/// The longest message the check writes. A longer one ends in `...`.
const CAPACITY: usize = 1024;

/// A message built in a `const` context, where no allocation is possible.
#[derive(Clone, Copy)]
pub struct Message {
    bytes: [u8; CAPACITY],
    len: usize,
    cut: bool,
}

impl Message {
    /// An empty message.
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            bytes: [0; CAPACITY],
            len: 0,
            cut: false,
        }
    }

    /// Appends `text`, cutting it at a character boundary when the buffer is full.
    #[must_use]
    pub(crate) const fn push(mut self, text: &str) -> Self {
        if self.cut {
            return self;
        }
        let text = text.as_bytes();
        let mut index = 0;
        while index < text.len() {
            if self.len == CAPACITY {
                // Back off to a character boundary and mark the cut.
                let mut end = CAPACITY - 3;
                while end > 0 && (self.bytes[end] & 0b1100_0000) == 0b1000_0000 {
                    end -= 1;
                }
                self.bytes[end] = b'.';
                self.bytes[end + 1] = b'.';
                self.bytes[end + 2] = b'.';
                self.len = end + 3;
                self.cut = true;
                return self;
            }
            self.bytes[self.len] = text[index];
            self.len += 1;
            index += 1;
        }
        self
    }

    /// Appends `text` between backticks.
    #[must_use]
    pub(crate) const fn code(self, text: &str) -> Self {
        self.push("`").push(text).push("`")
    }

    /// Appends a decimal integer.
    #[must_use]
    pub(crate) const fn int(self, value: i128) -> Self {
        let mut digits = [0u8; 40];
        let mut at = digits.len();
        let negative = value < 0;
        let mut rest = value.unsigned_abs();
        loop {
            at -= 1;
            // The remainder of a division by ten is a single digit.
            #[allow(clippy::cast_possible_truncation)]
            let digit = (rest % 10) as u8;
            digits[at] = b'0' + digit;
            rest /= 10;
            if rest == 0 {
                break;
            }
        }
        if negative {
            at -= 1;
            digits[at] = b'-';
        }
        let (_, tail) = digits.split_at(at);
        match core::str::from_utf8(tail) {
            Ok(text) => self.push(text),
            Err(_) => self,
        }
    }

    /// The text written so far.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        let (text, _) = self.bytes.split_at(self.len);
        match core::str::from_utf8(text) {
            Ok(text) => text,
            Err(_) => "the schema check wrote a message that is not UTF-8",
        }
    }
}

impl core::fmt::Debug for Message {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One step from a value into a part of it.
#[derive(Clone, Copy)]
pub(crate) enum Step<'a> {
    /// The checked type itself.
    Root(&'a str),
    /// A property, by the name serde gives it.
    Property(&'a str),
    /// The items of a sequence.
    Item,
    /// The values of a map.
    Value,
    /// A variant of an enum, by the name serde gives it.
    Variant(&'a str),
}

/// Where in the checked type the comparison stands: a chain of steps up to the root, kept on the
/// comparison's own stack.
#[derive(Clone, Copy)]
pub(crate) struct Path<'a> {
    step: Step<'a>,
    parent: Option<&'a Self>,
}

impl<'a> Path<'a> {
    /// The checked type.
    pub(crate) const fn root(name: &'a str) -> Self {
        Self {
            step: Step::Root(name),
            parent: None,
        }
    }

    /// One step further in.
    pub(crate) const fn then(&'a self, step: Step<'a>) -> Self {
        Self {
            step,
            parent: Some(self),
        }
    }

    /// Writes the path: `Order.items[].qty`, `Order.tags{}`, `Order.payment<Crypto>`.
    pub(crate) const fn write(&self, message: Message) -> Message {
        let message = match self.parent {
            Some(parent) => parent.write(message),
            None => message,
        };
        match self.step {
            Step::Root(name) => message.push(name),
            Step::Property(name) => message.push(".").push(name),
            Step::Item => message.push("[]"),
            Step::Value => message.push("{}"),
            Step::Variant(name) => message.push("<").push(name).push(">"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_reads_from_the_type_down() {
        let root = Path::root("Order");
        let items = root.then(Step::Property("items"));
        let item = items.then(Step::Item);
        let qty = item.then(Step::Property("qty"));
        assert_eq!(qty.write(Message::new()).as_str(), "Order.items[].qty");

        let tags = root.then(Step::Property("tags"));
        let value = tags.then(Step::Value);
        assert_eq!(value.write(Message::new()).as_str(), "Order.tags{}");

        let payment = root.then(Step::Property("payment"));
        let crypto = payment.then(Step::Variant("Crypto"));
        assert_eq!(
            crypto.write(Message::new()).as_str(),
            "Order.payment<Crypto>"
        );
    }

    #[test]
    fn integers_are_written_in_decimal() {
        let message = Message::new()
            .int(0)
            .push(" ")
            .int(-128)
            .push(" ")
            .int(i128::from(u64::MAX));
        assert_eq!(message.as_str(), "0 -128 18446744073709551615");
    }

    #[test]
    fn a_message_longer_than_the_buffer_ends_in_an_ellipsis() {
        let mut message = Message::new();
        let mut round = 0;
        while round < 300 {
            message = message.push("é-x ");
            round += 1;
        }
        let text = message.as_str();
        assert!(text.ends_with("..."), "{text}");
        assert!(text.len() <= CAPACITY);
    }
}
