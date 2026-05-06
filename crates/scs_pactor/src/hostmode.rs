use crate::ScsPactorError;

const FRAME_SYNC: [u8; 2] = [0xAA, 0xAA];
const STUFF_BYTE: u8 = 0x00;
const REQUEST_PACKET: [u8; 4] = [0xAA, 0xAA, 0xAA, 0x55];
const MAX_STANDARD_PAYLOAD_LEN: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostmodeFrame {
    pub channel: u8,
    pub code: u8,
    pub payload: Vec<u8>,
}

impl HostmodeFrame {
    pub fn new(channel: u8, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel,
            code: 0,
            payload: payload.into(),
        }
    }

    pub fn with_code(channel: u8, code: u8, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel,
            code,
            payload: payload.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostmodePacket {
    Frame(HostmodeFrame),
    RepeatRequest,
}

pub fn encode_frame(frame: &HostmodeFrame) -> Result<Vec<u8>, ScsPactorError> {
    if frame.payload.len() > MAX_STANDARD_PAYLOAD_LEN {
        return Err(ScsPactorError::ExceedsMtu(frame.payload.len()));
    }

    let mut body = Vec::with_capacity(frame.payload.len() + 5);
    body.push(frame.channel);
    body.push(frame.code);
    body.push(length_minus_one(frame.payload.len())?);
    body.extend_from_slice(&frame.payload);

    let crc = crc16_ccitt_false(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let mut encoded = Vec::with_capacity(body.len() + 2);
    encoded.extend_from_slice(&FRAME_SYNC);
    stuff_bytes(&body, &mut encoded);
    Ok(encoded)
}

pub fn encode_repeat_request() -> Vec<u8> {
    REQUEST_PACKET.to_vec()
}

pub fn decode_frame(bytes: &[u8]) -> Result<HostmodeFrame, ScsPactorError> {
    match decode_packet(bytes)? {
        HostmodePacket::Frame(frame) => Ok(frame),
        HostmodePacket::RepeatRequest => Err(ScsPactorError::Protocol(
            "hostmode repeat request is not a data frame".to_owned(),
        )),
    }
}

pub fn decode_packet(bytes: &[u8]) -> Result<HostmodePacket, ScsPactorError> {
    if bytes == REQUEST_PACKET {
        return Ok(HostmodePacket::RepeatRequest);
    }
    if bytes.len() < 7 {
        return Err(ScsPactorError::Protocol(
            "hostmode frame too short".to_owned(),
        ));
    }
    if !bytes.starts_with(&FRAME_SYNC) {
        return Err(ScsPactorError::Protocol(
            "hostmode frame sync missing".to_owned(),
        ));
    }

    let body = destuff_bytes(&bytes[2..])?;
    if body.len() < 5 {
        return Err(ScsPactorError::Protocol(
            "hostmode frame body too short".to_owned(),
        ));
    }

    let crc_offset = body.len() - 2;
    let expected_crc = u16::from_le_bytes([body[crc_offset], body[crc_offset + 1]]);
    let actual_crc = crc16_ccitt_false(&body[..crc_offset]);
    if actual_crc != expected_crc {
        return Err(ScsPactorError::Protocol(
            "hostmode frame crc mismatch".to_owned(),
        ));
    }

    let payload_len = payload_len_from_minus_one(body[2]);
    let expected_body_len = 3 + payload_len + 2;
    if body.len() != expected_body_len {
        return Err(ScsPactorError::Protocol(format!(
            "hostmode frame length mismatch: expected body {expected_body_len}, got {}",
            body.len()
        )));
    }

    Ok(HostmodePacket::Frame(HostmodeFrame {
        channel: body[0],
        code: body[1],
        payload: body[3..crc_offset].to_vec(),
    }))
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
        match self.next_packet()? {
            Some(HostmodePacket::Frame(frame)) => Ok(Some(frame)),
            Some(HostmodePacket::RepeatRequest) => Err(ScsPactorError::Protocol(
                "hostmode repeat request is not a data frame".to_owned(),
            )),
            None => Ok(None),
        }
    }

    pub fn next_packet(&mut self) -> Result<Option<HostmodePacket>, ScsPactorError> {
        let Some(start) = find_sync(&self.buffer) else {
            retain_possible_partial_sync(&mut self.buffer);
            return Ok(None);
        };
        if start > 0 {
            self.buffer.drain(..start);
        }

        if self.buffer.starts_with(&REQUEST_PACKET) {
            self.buffer.drain(..REQUEST_PACKET.len());
            return Ok(Some(HostmodePacket::RepeatRequest));
        }

        if self.buffer.len() < 5 {
            return Ok(None);
        }

        let Some(body_len) = decoded_body_len(&self.buffer[2..])? else {
            return Ok(None);
        };
        let Some(raw_len) = raw_len_for_destuffed_body(&self.buffer[2..], body_len)? else {
            return Ok(None);
        };

        let packet_len = 2 + raw_len;
        let packet: Vec<u8> = self.buffer.drain(..packet_len).collect();
        decode_packet(&packet).map(Some)
    }
}

fn length_minus_one(payload_len: usize) -> Result<u8, ScsPactorError> {
    if payload_len == 0 {
        return Err(ScsPactorError::Protocol(
            "hostmode payload cannot be empty".to_owned(),
        ));
    }
    if payload_len > MAX_STANDARD_PAYLOAD_LEN {
        return Err(ScsPactorError::ExceedsMtu(payload_len));
    }
    Ok((payload_len - 1) as u8)
}

fn payload_len_from_minus_one(length_byte: u8) -> usize {
    length_byte as usize + 1
}

fn stuff_bytes(input: &[u8], output: &mut Vec<u8>) {
    for byte in input {
        output.push(*byte);
        if *byte == 0xAA {
            output.push(STUFF_BYTE);
        }
    }
}

fn destuff_bytes(input: &[u8]) -> Result<Vec<u8>, ScsPactorError> {
    let mut output = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        let byte = input[i];
        output.push(byte);
        i += 1;
        if byte == 0xAA {
            match input.get(i) {
                Some(0x00) => i += 1,
                Some(_) => {
                    return Err(ScsPactorError::Protocol(
                        "hostmode byte-stuffing error".to_owned(),
                    ))
                }
                None => {
                    return Err(ScsPactorError::Protocol(
                        "hostmode truncated stuffed byte".to_owned(),
                    ))
                }
            }
        }
    }
    Ok(output)
}

fn find_sync(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == FRAME_SYNC)
}

fn retain_possible_partial_sync(buffer: &mut Vec<u8>) {
    if buffer.last() == Some(&0xAA) {
        buffer.drain(..buffer.len() - 1);
    } else {
        buffer.clear();
    }
}

fn decoded_body_len(raw_body: &[u8]) -> Result<Option<usize>, ScsPactorError> {
    let Some(header) = destuffed_prefix(raw_body, 3)? else {
        return Ok(None);
    };
    Ok(Some(3 + payload_len_from_minus_one(header[2]) + 2))
}

fn raw_len_for_destuffed_body(
    raw_body: &[u8],
    expected_destuffed_len: usize,
) -> Result<Option<usize>, ScsPactorError> {
    let mut destuffed = 0;
    let mut raw = 0;
    while raw < raw_body.len() && destuffed < expected_destuffed_len {
        let byte = raw_body[raw];
        raw += 1;
        destuffed += 1;
        if byte == 0xAA {
            match raw_body.get(raw) {
                Some(0x00) => raw += 1,
                Some(_) => {
                    return Err(ScsPactorError::Protocol(
                        "hostmode byte-stuffing error".to_owned(),
                    ))
                }
                None => return Ok(None),
            }
        }
    }

    if destuffed == expected_destuffed_len {
        Ok(Some(raw))
    } else {
        Ok(None)
    }
}

fn destuffed_prefix(raw_body: &[u8], len: usize) -> Result<Option<Vec<u8>>, ScsPactorError> {
    let mut output = Vec::with_capacity(len);
    let mut raw = 0;
    while raw < raw_body.len() && output.len() < len {
        let byte = raw_body[raw];
        output.push(byte);
        raw += 1;
        if byte == 0xAA {
            match raw_body.get(raw) {
                Some(0x00) => raw += 1,
                Some(_) => {
                    return Err(ScsPactorError::Protocol(
                        "hostmode byte-stuffing error".to_owned(),
                    ))
                }
                None => return Ok(None),
            }
        }
    }

    if output.len() == len {
        Ok(Some(output))
    } else {
        Ok(None)
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
        let frame = HostmodeFrame::with_code(1, b'C', b"DL1ZAM".to_vec());
        let encoded = encode_frame(&frame).unwrap();
        assert!(encoded.starts_with(&FRAME_SYNC));
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hostmode_frame_stuffs_0xaa_bytes() {
        let frame = HostmodeFrame::with_code(2, 0, vec![0x01, 0xAA, 0x02]);
        let encoded = encode_frame(&frame).unwrap();
        assert!(encoded.windows(2).any(|window| window == [0xAA, 0x00]));
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hostmode_frame_rejects_bad_crc() {
        let frame = HostmodeFrame::with_code(2, 0, b"payload".to_vec());
        let mut encoded = encode_frame(&frame).unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0x01;
        let err = decode_frame(&encoded).unwrap_err();
        assert!(matches!(err, ScsPactorError::Protocol(_)));
    }

    #[test]
    fn hostmode_frame_rejects_empty_payload() {
        let frame = HostmodeFrame::with_code(2, b'D', Vec::new());
        let err = encode_frame(&frame).unwrap_err();
        assert!(matches!(err, ScsPactorError::Protocol(_)));
    }

    #[test]
    fn decoder_resyncs_after_garbage() {
        let frame = HostmodeFrame::with_code(7, b'D', b"X".to_vec());
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
    fn decoder_recognizes_repeat_request() {
        let mut decoder = HostmodeDecoder::new();
        decoder.push(&encode_repeat_request());
        assert_eq!(
            decoder.next_packet().unwrap(),
            Some(HostmodePacket::RepeatRequest)
        );
    }

    #[test]
    fn crc_known_vector() {
        assert_eq!(crc16_ccitt_false(b"123456789"), 0x29B1);
    }
}
