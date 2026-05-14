use crate::ScsPactorError;

const FRAME_SYNC: [u8; 2] = [0xAA, 0xAA];
const STUFF_BYTE: u8 = 0x00;
const REQUEST_PACKET: [u8; 4] = [0xAA, 0xAA, 0xAA, 0x55];
const MAX_STANDARD_PAYLOAD_LEN: usize = 256;

/// PACTOR channel used by ptc-go for the main data/command stream.
pub const PACTOR_CHANNEL: u8 = 31;

/// Type byte values for JHOST4 CRC hostmode (matches ptc-go).
///
/// The type byte sits in the second position of the frame body
/// (after channel, before length). In the original code this was called
/// `code` and held arbitrary command letters like `b'G'` or `b'I'`.
/// The real SCS protocol uses it to distinguish data vs command frames.
pub const TYPE_DATA: u8 = 0x00;
pub const TYPE_COMMAND: u8 = 0x01;
pub const TYPE_DATA_COUNTER: u8 = 0x80;
pub const TYPE_COMMAND_COUNTER: u8 = 0x81;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostmodeFrame {
    pub channel: u8,
    /// Type byte: TYPE_DATA (0x00), TYPE_COMMAND (0x01), or counter variants.
    pub code: u8,
    pub payload: Vec<u8>,
}

impl HostmodeFrame {
    /// Data frame (type = 0x00).
    pub fn new(channel: u8, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel,
            code: TYPE_DATA,
            payload: payload.into(),
        }
    }

    /// Frame with an explicit type byte.
    pub fn with_code(channel: u8, code: u8, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel,
            code,
            payload: payload.into(),
        }
    }

    /// Command frame (type = 0x01) with the command string as payload.
    ///
    /// In ptc-go hostmode, the command letter (e.g. `I`, `C`, `D`, `G`)
    /// is part of the payload, not the type byte. So `MYCALL N0CALL` is:
    /// `HostmodeFrame::command(31, b"I N0CALL")`.
    pub fn command(channel: u8, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel,
            code: TYPE_COMMAND,
            payload: payload.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostmodePacket {
    Frame(HostmodeFrame),
    RepeatRequest,
}

/// Encode a hostmode frame using JHOST4 CRC framing (matching ptc-go).
///
/// Wire format: `[0xAA, 0xAA] + stuffed(channel + type + len-1 + payload + crc)`
///
/// CRC is CRC16-CCITT (init 0x0000, poly 0x1021), then byte-reversed,
/// then encoded big-endian.
pub fn encode_frame(frame: &HostmodeFrame) -> Result<Vec<u8>, ScsPactorError> {
    if frame.payload.len() > MAX_STANDARD_PAYLOAD_LEN {
        return Err(ScsPactorError::ExceedsMtu(frame.payload.len()));
    }

    let mut body = Vec::with_capacity(frame.payload.len() + 5);
    body.push(frame.channel);
    body.push(frame.code);
    body.push(length_minus_one(frame.payload.len())?);
    body.extend_from_slice(&frame.payload);

    let crc = checksum(&body);
    body.extend_from_slice(&crc);

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
    if bytes.len() < 6 {
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
    if body.len() < 4 {
        return Err(ScsPactorError::Protocol(
            "hostmode frame body too short".to_owned(),
        ));
    }

    let crc_offset = body.len() - 2;
    let expected_crc = [body[crc_offset], body[crc_offset + 1]];
    let actual_crc = checksum(&body[..crc_offset]);
    if actual_crc != expected_crc {
        return Err(ScsPactorError::Protocol(
            "hostmode frame crc mismatch".to_owned(),
        ));
    }

    // The PTC-IIpro sends frames WITHOUT a length byte:
    //   [channel][type][payload...][CRC]
    // But ptc-go/Dragon frames include a length-1 byte:
    //   [channel][type][length-1][payload...][CRC]
    //
    // We detect which format by checking if body[2] as length-1 is
    // consistent with the actual body size. If it matches, we have
    // the length-byte format; otherwise, we treat the entire body
    // (minus channel, type, CRC) as payload.
    let payload_start;
    if crc_offset >= 3 {
        let claimed_len = payload_len_from_minus_one(body[2]);
        let expected_body_len = 3 + claimed_len + 2;
        if body.len() == expected_body_len {
            // Length-byte format (ptc-go / Dragon)
            payload_start = 3;
        } else {
            // No length byte (PTC-IIpro)
            payload_start = 2;
        }
    } else {
        payload_start = 2;
    }

    Ok(HostmodePacket::Frame(HostmodeFrame {
        channel: body[0],
        code: body[1],
        payload: body[payload_start..crc_offset].to_vec(),
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

        // Minimum frame: [AA AA] + channel + type + CRC(2) = 6 raw bytes
        // (more with byte-stuffing). Try length-byte format first, then
        // scan for CRC match at each possible boundary.

        // Try length-byte format (ptc-go / Dragon)
        if self.buffer.len() >= 7 {
            if let Ok(Some(body_len)) = decoded_body_len(&self.buffer[2..]) {
                if let Ok(Some(raw_len)) =
                    raw_len_for_destuffed_body(&self.buffer[2..], body_len)
                {
                    let packet_len = 2 + raw_len;
                    if self.buffer.len() >= packet_len {
                        let candidate: Vec<u8> =
                            self.buffer[..packet_len].to_vec();
                        if decode_packet(&candidate).is_ok() {
                            self.buffer.drain(..packet_len);
                            return decode_packet(&candidate).map(Some);
                        }
                    }
                }
            }
        }

        // Scan for CRC-valid frame without length byte.
        // Try each possible raw frame length from smallest to largest.
        // Min destuffed body = 4 (channel + type + 2 CRC bytes).
        for raw_end in 6..=self.buffer.len() {
            let candidate = &self.buffer[..raw_end];
            if let Ok(pkt) = decode_packet(candidate) {
                self.buffer.drain(..raw_end);
                return Ok(Some(pkt));
            }
        }

        // Not enough data yet, or no valid frame found in buffer.
        // Keep at most 512 bytes to prevent unbounded growth.
        if self.buffer.len() > 512 {
            self.buffer.drain(..self.buffer.len() - 512);
        }
        Ok(None)
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

/// CRC16-CCITT as implemented by `github.com/howeyc/crc16.ChecksumCCITT`.
///
/// ptc-go depends on `github.com/howeyc/crc16`, whose `ChecksumCCITT`
/// matches the reflected CCITT polynomial `0x8408`, init `0xffff`,
/// xorout `0xffff`.
fn crc16_ccitt(bytes: &[u8]) -> u16 {
    let mut crc = 0xffffu16;
    for byte in bytes {
        crc ^= *byte as u16;
        for _ in 0..8 {
            if crc & 0x0001 != 0 {
                crc = (crc >> 1) ^ 0x8408;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xffff
}

/// Compute the 2-byte CRC checksum for a hostmode frame body.
///
/// Matches ptc-go: howeyc CRC16-CCITT, then `bits.ReverseBytes16`,
/// then big-endian. This is equivalent to writing the checksum little-endian.
fn checksum(body: &[u8]) -> [u8; 2] {
    let crc = crc16_ccitt(body);
    let reversed = crc.swap_bytes();
    reversed.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_known_vector() {
        assert_eq!(crc16_ccitt(b"123456789"), 0x906e);
    }

    #[test]
    fn checksum_applies_reverse_and_big_endian() {
        let body = [0x1F, 0x01, 0x00, 0x47];
        let crc = crc16_ccitt(&body);
        let reversed = crc.swap_bytes();
        let expected = reversed.to_be_bytes();
        assert_eq!(checksum(&body), expected);
    }

    #[test]
    fn hostmode_frame_round_trip() {
        // Command frame: channel 31, type COMMAND, payload "C DL1ZAM"
        let frame = HostmodeFrame::command(PACTOR_CHANNEL, b"C DL1ZAM".to_vec());
        let encoded = encode_frame(&frame).unwrap();
        assert!(encoded.starts_with(&FRAME_SYNC));
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hostmode_data_frame_round_trip() {
        let frame = HostmodeFrame::new(PACTOR_CHANNEL, b"hello".to_vec());
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(decoded.code, TYPE_DATA);
    }

    #[test]
    fn hostmode_frame_stuffs_0xaa_bytes() {
        let frame = HostmodeFrame::with_code(2, TYPE_DATA, vec![0x01, 0xAA, 0x02]);
        let encoded = encode_frame(&frame).unwrap();
        assert!(encoded.windows(2).any(|window| window == [0xAA, 0x00]));
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hostmode_frame_rejects_bad_crc() {
        let frame = HostmodeFrame::with_code(2, TYPE_DATA, b"payload".to_vec());
        let mut encoded = encode_frame(&frame).unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0x01;
        let err = decode_frame(&encoded).unwrap_err();
        assert!(matches!(err, ScsPactorError::Protocol(_)));
    }

    #[test]
    fn hostmode_frame_rejects_empty_payload() {
        let frame = HostmodeFrame::command(2, Vec::new());
        let err = encode_frame(&frame).unwrap_err();
        assert!(matches!(err, ScsPactorError::Protocol(_)));
    }

    #[test]
    fn decoder_resyncs_after_garbage() {
        let frame = HostmodeFrame::command(7, b"D".to_vec());
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
    fn poll_command_encodes_correctly() {
        // A poll on channel 31: type=COMMAND, payload="G" (the poll command)
        let frame = HostmodeFrame::command(PACTOR_CHANNEL, b"G".to_vec());
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded.channel, PACTOR_CHANNEL);
        assert_eq!(decoded.code, TYPE_COMMAND);
        assert_eq!(decoded.payload, b"G");
    }

    #[test]
    fn hostmode_quit_matches_ptc_go_crc() {
        let frame = HostmodeFrame::command(0, b"JHOST0".to_vec());
        let encoded = encode_frame(&frame).unwrap();
        assert_eq!(
            encoded,
            vec![
                0xaa, 0xaa, 0x00, 0x01, 0x05, 0x4a, 0x48, 0x4f, 0x53, 0x54, 0x30, 0xfb, 0x3d,
            ]
        );
    }

    #[test]
    fn status_poll_matches_ptc_go_debug_dump() {
        let frame = HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec());
        let encoded = encode_frame(&frame).unwrap();
        assert_eq!(
            encoded,
            vec![0xaa, 0xaa, 0x1f, 0x01, 0x00, 0x4c, 0x32, 0x5f]
        );
    }
}
