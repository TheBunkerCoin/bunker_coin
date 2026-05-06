use crate::ScsPactorError;

const FRAME_START: u8 = 0xAA;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostmodeFrame {
    pub channel: u8,
    pub payload: Vec<u8>,
}

impl HostmodeFrame {
    pub fn new(channel: u8, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel,
            payload: payload.into(),
        }
    }
}

pub fn encode_frame(frame: &HostmodeFrame) -> Result<Vec<u8>, ScsPactorError> {
    if frame.payload.len() > u16::MAX as usize {
        return Err(ScsPactorError::ExceedsMtu(frame.payload.len()));
    }

    let len = frame.payload.len() as u16;
    let mut encoded = Vec::with_capacity(frame.payload.len() + 6);
    encoded.push(FRAME_START);
    encoded.push(frame.channel);
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(&frame.payload);

    let crc = crc16_ccitt_false(&encoded[1..]);
    encoded.extend_from_slice(&crc.to_be_bytes());
    Ok(encoded)
}

pub fn decode_frame(bytes: &[u8]) -> Result<HostmodeFrame, ScsPactorError> {
    if bytes.len() < 6 {
        return Err(ScsPactorError::Protocol(
            "hostmode frame too short".to_owned(),
        ));
    }
    if bytes[0] != FRAME_START {
        return Err(ScsPactorError::Protocol(
            "hostmode frame start missing".to_owned(),
        ));
    }

    let channel = bytes[1];
    let len = u16::from_be_bytes([bytes[2], bytes[3]]) as usize;
    let expected_len = 1 + 1 + 2 + len + 2;
    if bytes.len() != expected_len {
        return Err(ScsPactorError::Protocol(format!(
            "hostmode frame length mismatch: expected {expected_len}, got {}",
            bytes.len()
        )));
    }

    let crc_offset = bytes.len() - 2;
    let expected_crc = u16::from_be_bytes([bytes[crc_offset], bytes[crc_offset + 1]]);
    let actual_crc = crc16_ccitt_false(&bytes[1..crc_offset]);
    if actual_crc != expected_crc {
        return Err(ScsPactorError::Protocol(
            "hostmode frame crc mismatch".to_owned(),
        ));
    }

    Ok(HostmodeFrame {
        channel,
        payload: bytes[4..crc_offset].to_vec(),
    })
}

#[derive(Debug, Default)]
pub struct HostmodeDecoder {
    buffer: Vec<u8>,
}

impl HostmodeDecoder {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    pub fn next_frame(&mut self) -> Result<Option<HostmodeFrame>, ScsPactorError> {
        let Some(start) = self.buffer.iter().position(|byte| *byte == FRAME_START) else {
            self.buffer.clear();
            return Ok(None);
        };
        if start > 0 {
            self.buffer.drain(..start);
        }

        if self.buffer.len() < 4 {
            return Ok(None);
        }

        let payload_len = u16::from_be_bytes([self.buffer[2], self.buffer[3]]) as usize;
        let frame_len = 1 + 1 + 2 + payload_len + 2;
        if self.buffer.len() < frame_len {
            return Ok(None);
        }

        let frame_bytes: Vec<u8> = self.buffer.drain(..frame_len).collect();
        decode_frame(&frame_bytes).map(Some)
    }
}

fn crc16_ccitt_false(bytes: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for byte in bytes {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostmode_frame_round_trip() {
        let frame = HostmodeFrame::new(1, b"MYCALL N0CALL".to_vec());
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hostmode_frame_rejects_bad_crc() {
        let frame = HostmodeFrame::new(2, b"payload".to_vec());
        let mut encoded = encode_frame(&frame).unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0x01;
        let err = decode_frame(&encoded).unwrap_err();
        assert!(matches!(err, ScsPactorError::Protocol(_)));
    }

    #[test]
    fn decoder_resyncs_after_garbage() {
        let frame = HostmodeFrame::new(7, b"D".to_vec());
        let encoded = encode_frame(&frame).unwrap();

        let mut decoder = HostmodeDecoder::new();
        decoder.push(&[0x00, 0x01, 0x02, 0x03]);
        assert_eq!(decoder.next_frame().unwrap(), None);

        decoder.push(&encoded[..3]);
        assert_eq!(decoder.next_frame().unwrap(), None);

        decoder.push(&encoded[3..]);
        assert_eq!(decoder.next_frame().unwrap(), Some(frame));
    }

    #[test]
    fn crc_known_vector() {
        assert_eq!(crc16_ccitt_false(b"123456789"), 0x29B1);
    }
}
