//! A simple ASCII protocol.

use std::fmt::Debug;

#[derive(Default)]
pub struct Parser {
    buffer: Vec<u8>,
    pos: usize,
}

impl Parser {
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.drain(0..self.pos);
        self.pos = 0;
        self.buffer.extend_from_slice(data);
    }

    pub fn next<'a>(&'a mut self) -> Option<Message<'a>> {
        // Find next line feed
        let nl = self.buffer[self.pos..].iter().position(|&c| c == b'\n');
        let nl = match nl {
            None => return None,
            Some(p) => p + self.pos,
        };

        // Build a Message
        let msg = Message::new(&self.buffer[self.pos..nl]);

        // Update position
        self.pos = nl + 1;

        Some(msg)
    }

    pub fn is_empty(&self) -> bool {
        self.buffer[self.pos..].is_empty()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Message<'a>(Vec<&'a [u8]>);

impl<'a> Debug for Message<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Message({:?}", self.0[0])?;
        for arg in &self.0[1..] {
            write!(f, ", {:?}", arg)?;
        }
        write!(f, ")")
    }
}

impl<'a> Message<'a> {
    fn new(line: &'a [u8]) -> Message<'a> {
        let mut args = Vec::new();
        let mut pos = 0;
        loop {
            while line[pos] == b' ' {
                pos += 1;
                if pos == line.len() {
                    return Message(args);
                }
            }
            let mut end = pos;
            while end < line.len() && line[end] != b' ' {
                end += 1;
            }
            args.push(&line[pos..end]);
            pos = end + 1;
            if pos >= line.len() {
                return Message(args);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get_bytes(&self, idx: usize) -> &'a [u8] {
        self.0[idx]
    }

    pub fn get_str(&self, idx: usize) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.0[idx])
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;

    #[test]
    fn test_parser() {
        let mut parser = Parser::default();

        parser.feed(b"FOO a");
        assert_eq!(parser.next(), None);
        assert!(!parser.is_empty());

        parser.feed(b"b 42\nBAR c\nEXI");
        let message = if let Some(m) = parser.next() { m } else { panic!() };
        assert_eq!(message.len(), 3);
        assert_eq!(message.get_bytes(0), b"FOO");
        assert_eq!(message.get_str(1), Ok("ab"));
        assert_eq!(message.get_str(2).unwrap().parse::<i32>().unwrap(), 42);
        let message = if let Some(m) = parser.next() { m } else { panic!() };
        assert_eq!(message.len(), 2);
        assert_eq!(message.get_str(0), Ok("BAR"));
        assert_eq!(message.get_bytes(1), b"c");
        assert_eq!(parser.next(), None);
        assert!(!parser.is_empty());

        parser.feed(b"T\n");
        assert!(!parser.is_empty());
        let message = if let Some(m) = parser.next() { m } else { panic!() };
        assert_eq!(message.len(), 1);
        assert!(parser.is_empty());
    }
}
