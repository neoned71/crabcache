use bytes::{Bytes, BytesMut};
use std::str;

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<Frame>>),
    Null,
    Boolean(bool),
    Double(f64),
    Map(Vec<(Frame, Frame)>),
    Set(Vec<Frame>),
}

impl Frame {
    pub fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Frame::SimpleString(s) => {
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Frame::Error(s) => {
                buf.extend_from_slice(b"-");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Frame::Integer(n) => {
                buf.extend_from_slice(b":");
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Frame::BulkString(None) => buf.extend_from_slice(b"$-1\r\n"),
            Frame::BulkString(Some(b)) => {
                buf.extend_from_slice(b"$");
                buf.extend_from_slice(b.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(b);
                buf.extend_from_slice(b"\r\n");
            }
            Frame::Array(None) => buf.extend_from_slice(b"*-1\r\n"),
            Frame::Array(Some(items)) => {
                buf.extend_from_slice(b"*");
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for f in items {
                    f.serialize(buf);
                }
            }
            Frame::Null => buf.extend_from_slice(b"_\r\n"),
            Frame::Boolean(b) => {
                buf.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
            }
            Frame::Double(d) => {
                buf.extend_from_slice(b",");
                buf.extend_from_slice(d.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Frame::Map(pairs) => {
                buf.extend_from_slice(b"%");
                buf.extend_from_slice(pairs.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (k, v) in pairs {
                    k.serialize(buf);
                    v.serialize(buf);
                }
            }
            Frame::Set(items) => {
                buf.extend_from_slice(b"~");
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for f in items {
                    f.serialize(buf);
                }
            }
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Frame::SimpleString(s) | Frame::Error(s) => Some(s.as_str()),
            Frame::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<Bytes> {
        match self {
            Frame::BulkString(Some(b)) => Some(b.clone()),
            Frame::SimpleString(s) => Some(Bytes::from(s.clone())),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Frame::Integer(n) => Some(*n),
            Frame::BulkString(Some(b)) => std::str::from_utf8(b).ok()?.parse().ok(),
            Frame::SimpleString(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn to_command_args(&self) -> Option<Vec<Bytes>> {
        match self {
            Frame::Array(Some(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    match it {
                        Frame::BulkString(Some(b)) => out.push(b.clone()),
                        Frame::SimpleString(s) => out.push(Bytes::from(s.clone())),
                        Frame::Integer(n) => out.push(Bytes::from(n.to_string())),
                        _ => return None,
                    }
                }
                Some(out)
            }
            _ => None,
        }
    }
}

fn find_crlf(data: &[u8], start: usize) -> Option<usize> {
    if start + 1 > data.len() {
        return None;
    }
    for i in start..data.len().saturating_sub(1) {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            return Some(i);
        }
    }
    // check last pair
    if data.len() >= 2 && data[data.len() - 2] == b'\r' && data[data.len() - 1] == b'\n' {
        return Some(data.len() - 2);
    }
    None
}

/// Parse a frame from data. Returns Ok(Some((frame, consumed))) if complete.
pub fn parse(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    if data.is_empty() {
        return Ok(None);
    }
    match data[0] {
        b'+' => parse_simple(data, Frame::SimpleString),
        b'-' => parse_simple(data, Frame::Error),
        b':' => parse_integer(data),
        b'$' => parse_bulk(data),
        b'*' => parse_array(data),
        b'_' => parse_null(data),
        b'#' => parse_bool(data),
        b',' => parse_double(data),
        b'%' => parse_map(data),
        b'~' => parse_set(data),
        b'>' => parse_push(data),
        _ => Err(format!("unknown type byte: {}", data[0] as char)),
    }
}

fn parse_simple(
    data: &[u8],
    ctor: impl FnOnce(String) -> Frame,
) -> Result<Option<(Frame, usize)>, String> {
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let s = std::str::from_utf8(&data[1..crlf])
        .map_err(|_| "invalid utf8".to_string())?
        .to_string();
    Ok(Some((ctor(s), crlf + 2)))
}

fn parse_integer(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let s = std::str::from_utf8(&data[1..crlf]).map_err(|_| "invalid utf8".to_string())?;
    let n: i64 = s.parse().map_err(|_| "invalid integer".to_string())?;
    Ok(Some((Frame::Integer(n), crlf + 2)))
}

fn parse_bulk(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let len_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| "invalid utf8".to_string())?;
    let len: i64 = len_str.parse().map_err(|_| "invalid length".to_string())?;
    if len < 0 {
        return Ok(Some((Frame::BulkString(None), crlf + 2)));
    }
    let len = len as usize;
    let data_start = crlf + 2;
    let data_end = data_start + len;
    let total = data_end + 2;
    if data.len() < total {
        return Ok(None);
    }
    if data[data_end] != b'\r' || data[data_end + 1] != b'\n' {
        return Err("missing crlf after bulk".to_string());
    }
    let bytes = Bytes::copy_from_slice(&data[data_start..data_end]);
    Ok(Some((Frame::BulkString(Some(bytes)), total)))
}

fn parse_array(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let len_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| "invalid utf8".to_string())?;
    let len: i64 = len_str.parse().map_err(|_| "invalid length".to_string())?;
    if len < 0 {
        return Ok(Some((Frame::Array(None), crlf + 2)));
    }
    let mut pos = crlf + 2;
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        match parse(&data[pos..])? {
            Some((f, consumed)) => {
                items.push(f);
                pos += consumed;
            }
            None => return Ok(None),
        }
    }
    Ok(Some((Frame::Array(Some(items)), pos)))
}

fn parse_null(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    if data.len() < 3 {
        return Ok(None);
    }
    if &data[0..3] != b"_\r\n" {
        return Err("bad null".to_string());
    }
    Ok(Some((Frame::Null, 3)))
}

fn parse_bool(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    if data.len() < 4 {
        return Ok(None);
    }
    if data[2] != b'\r' || data[3] != b'\n' {
        return Ok(None);
    }
    let b = match data[1] {
        b't' => true,
        b'f' => false,
        _ => return Err("bad bool".to_string()),
    };
    Ok(Some((Frame::Boolean(b), 4)))
}

fn parse_double(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let s = std::str::from_utf8(&data[1..crlf]).map_err(|_| "invalid utf8".to_string())?;
    let d: f64 = s.parse().map_err(|_| "invalid double".to_string())?;
    Ok(Some((Frame::Double(d), crlf + 2)))
}

fn parse_map(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let len_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| "invalid utf8".to_string())?;
    let len: i64 = len_str.parse().map_err(|_| "invalid length".to_string())?;
    if len < 0 {
        return Ok(Some((Frame::Map(vec![]), crlf + 2)));
    }
    let mut pos = crlf + 2;
    let mut pairs = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (k, c1) = match parse(&data[pos..])? {
            Some(x) => x,
            None => return Ok(None),
        };
        pos += c1;
        let (v, c2) = match parse(&data[pos..])? {
            Some(x) => x,
            None => return Ok(None),
        };
        pos += c2;
        pairs.push((k, v));
    }
    Ok(Some((Frame::Map(pairs), pos)))
}

fn parse_set(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let len_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| "invalid utf8".to_string())?;
    let len: i64 = len_str.parse().map_err(|_| "invalid length".to_string())?;
    if len < 0 {
        return Ok(Some((Frame::Set(vec![]), crlf + 2)));
    }
    let mut pos = crlf + 2;
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (f, c) = match parse(&data[pos..])? {
            Some(x) => x,
            None => return Ok(None),
        };
        items.push(f);
        pos += c;
    }
    Ok(Some((Frame::Set(items), pos)))
}

fn parse_push(data: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    // push -> array
    let crlf = match find_crlf(data, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let len_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| "invalid utf8".to_string())?;
    let len: i64 = len_str.parse().map_err(|_| "invalid length".to_string())?;
    if len < 0 {
        return Ok(Some((Frame::Array(None), crlf + 2)));
    }
    let mut pos = crlf + 2;
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (f, c) = match parse(&data[pos..])? {
            Some(x) => x,
            None => return Ok(None),
        };
        items.push(f);
        pos += c;
    }
    Ok(Some((Frame::Array(Some(items)), pos)))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_string() {
        let data = b"+OK\r\n";
        let (f, c) = parse(data).unwrap().unwrap();
        assert_eq!(f, Frame::SimpleString("OK".to_string()));
        assert_eq!(c, 5);
    }

    #[test]
    fn parse_bulk() {
        let data = b"$6\r\nfoobar\r\n";
        let (f, c) = parse(data).unwrap().unwrap();
        assert_eq!(f, Frame::BulkString(Some(bytes::Bytes::from("foobar"))));
        assert_eq!(c, 12);
    }

    #[test]
    fn parse_null_bulk() {
        let data = b"$-1\r\n";
        let (f, c) = parse(data).unwrap().unwrap();
        assert_eq!(f, Frame::BulkString(None));
        assert_eq!(c, 5);
    }

    #[test]
    fn parse_array() {
        let data = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (f, _) = parse(data).unwrap().unwrap();
        match f {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Frame::BulkString(Some(bytes::Bytes::from("foo"))));
                assert_eq!(items[1], Frame::BulkString(Some(bytes::Bytes::from("bar"))));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn parse_incomplete_returns_none() {
        let data = b"*2\r\n$3\r\nfoo\r\n";
        assert!(parse(data).unwrap().is_none());
    }

    #[test]
    fn roundtrip_serialize() {
        let f = Frame::Array(Some(vec![
            Frame::BulkString(Some(bytes::Bytes::from("SET"))),
            Frame::BulkString(Some(bytes::Bytes::from("k"))),
            Frame::BulkString(Some(bytes::Bytes::from("v"))),
        ]));
        let mut buf = bytes::BytesMut::new();
        f.serialize(&mut buf);
        let (parsed, _) = parse(&buf).unwrap().unwrap();
        assert_eq!(parsed, f);
    }
}
