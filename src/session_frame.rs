//! Bounded newline framing preserving bytes after a frame for persistent sessions.
use crate::{Error, Result};
use std::io::Read;
#[derive(Default)]
pub(crate) struct Frames {
    pending: Vec<u8>,
}
impl Frames {
    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
    pub(crate) fn next(&mut self, stream: &mut impl Read, limit: usize) -> Result<Option<Vec<u8>>> {
        let mut scanned = 0;
        loop {
            if let Some(offset) = self.pending[scanned..].iter().position(|b| *b == b'\n') {
                let end = scanned + offset;
                if end > limit {
                    return Err(Error::Storage("frame limit exceeded".into()));
                }
                let tail = self.pending.split_off(end + 1);
                let mut frame = std::mem::replace(&mut self.pending, tail);
                frame.pop();
                return Ok(Some(frame));
            }
            if self.pending.len() > limit {
                return Err(Error::Storage("frame limit exceeded".into()));
            }
            scanned = self.pending.len();
            let mut buffer = [0u8; 8192];
            let size = buffer.len().min(limit + 1 - self.pending.len());
            let n = stream
                .read(&mut buffer[..size])
                .map_err(|e| Error::Storage(e.to_string()))?;
            if n == 0 {
                return if self.pending.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::Storage(
                        "incomplete frame; outcome may be uncertain".into(),
                    ))
                };
            }
            self.pending.extend_from_slice(&buffer[..n]);
        }
    }
}
