//! Helpers for detecting covenant / introspection opcode usage in scripts.
//!
//! The covenant ("introspection") opcodes were added to kaspa-txscript and
//! occupy the contiguous byte range `OpTxVersion..=OpBlake3WithKey`
//! (`0xb2..=0xda`) — every opcode gated on `covenants_enabled`. They only ever
//! execute inside a script body, so for a standard pay-to-script-hash spend they
//! live in the redeem script, which is revealed as the final data push of the
//! signature script. For a non-standard output they may appear directly in the
//! script public key.

use kaspa_txscript::opcodes::codes;

/// First byte value of the covenant / introspection opcode range (`OpTxVersion`).
pub const COVENANT_OPCODE_MIN: u8 = codes::OpTxVersion;

/// Last byte value of the covenant / introspection opcode range
/// (`OpBlake3WithKey` — the final covenant-gated opcode, after the
/// `OpCheckSigFromStack{,ECDSA}` and `OpBlake3` additions).
pub const COVENANT_OPCODE_MAX: u8 = codes::OpBlake3WithKey;

/// Outcome of attempting to read a data push at the current cursor position.
enum PushScan {
    /// `op` is not a data-push opcode; continue scanning from `cursor`.
    NotPush,
    /// A data push whose payload is the given number of bytes. `cursor` has
    /// been advanced past any length prefix; the payload itself is not skipped.
    Data(usize),
    /// `op` is a push opcode but its length prefix runs past the end of the
    /// script, so the remainder is unparseable.
    Truncated,
}

/// Reads the data push starting at `op`, advancing `cursor` past any length
/// prefix. Distinguishes a non-push opcode from a truncated push so callers do
/// not re-interpret a dangling length byte as an opcode.
fn scan_push(op: u8, script: &[u8], cursor: &mut usize) -> PushScan {
    match op {
        0x01..=0x4b => PushScan::Data(op as usize),
        0x4c => match script.get(*cursor) {
            // OP_PUSHDATA1: next byte is the length
            Some(&len) => {
                *cursor += 1;
                PushScan::Data(len as usize)
            }
            None => PushScan::Truncated,
        },
        0x4d => match script.get(*cursor..*cursor + 2) {
            // OP_PUSHDATA2: next 2 bytes (little endian) are the length
            Some(bytes) => {
                *cursor += 2;
                PushScan::Data(u16::from_le_bytes([bytes[0], bytes[1]]) as usize)
            }
            None => PushScan::Truncated,
        },
        0x4e => match script.get(*cursor..*cursor + 4) {
            // OP_PUSHDATA4: next 4 bytes (little endian) are the length
            Some(bytes) => {
                *cursor += 4;
                PushScan::Data(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize)
            }
            None => PushScan::Truncated,
        },
        _ => PushScan::NotPush,
    }
}

/// Scans a single script body and returns true if it contains any opcode in the
/// covenant / introspection range. Data pushes are skipped so that bytes inside
/// pushed data are never mistaken for opcodes.
pub fn script_uses_introspection_opcode(script: &[u8]) -> bool {
    let mut cursor = 0;
    while cursor < script.len() {
        let op = script[cursor];
        cursor += 1;

        if (COVENANT_OPCODE_MIN..=COVENANT_OPCODE_MAX).contains(&op) {
            return true;
        }

        match scan_push(op, script, &mut cursor) {
            PushScan::Data(data_len) => cursor = cursor.saturating_add(data_len),
            // A truncated push leaves the rest of the script unparseable; stop
            // instead of re-reading length bytes as opcodes.
            PushScan::Truncated => break,
            PushScan::NotPush => {}
        }
    }

    false
}

/// Returns the data of the last push operation in `script`, or `None` if there
/// is no parseable push. For a P2SH signature script this is the redeem script.
fn last_push_payload(script: &[u8]) -> Option<&[u8]> {
    let mut cursor = 0;
    let mut last: Option<&[u8]> = None;

    while cursor < script.len() {
        let op = script[cursor];
        cursor += 1;

        match scan_push(op, script, &mut cursor) {
            PushScan::Data(data_len) => {
                let end = cursor.checked_add(data_len)?;
                if end > script.len() {
                    // Truncated push, stop parsing.
                    break;
                }
                last = Some(&script[cursor..end]);
                cursor = end;
            }
            PushScan::Truncated => break,
            PushScan::NotPush => continue,
        }
    }

    last
}

/// Returns true if the redeem script revealed by a signature script (its final
/// data push) uses any covenant / introspection opcode.
///
/// This treats the final push as a redeem script, so it is only meaningful for
/// pay-to-script-hash spends. Callers must gate on the spent output being P2SH;
/// applied to a plain pay-to-pubkey spend the final push is a signature and its
/// bytes would be misread as opcodes.
pub fn signature_script_reveals_introspection(signature_script: &[u8]) -> bool {
    match last_push_payload(signature_script) {
        Some(redeem) => script_uses_introspection_opcode(redeem),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_introspection_opcode() {
        // OpTxOutputCount (0xb4) standing alone
        assert!(script_uses_introspection_opcode(&[0xb4]));
        // OpTxVersion (0xb2), bottom of range
        assert!(script_uses_introspection_opcode(&[0xb2]));
        // OpBlake3WithKey (0xda), top of range
        assert!(script_uses_introspection_opcode(&[0xda]));
        // OpBlake3 (0xd9) and OpCheckSigFromStackECDSA (0xd8) inside the range
        assert!(script_uses_introspection_opcode(&[0xd9]));
        assert!(script_uses_introspection_opcode(&[0xd8]));
        // 0xdb (OpUnknown219) is just past the range and must not match
        assert!(!script_uses_introspection_opcode(&[0xdb]));
    }

    #[test]
    fn ignores_introspection_bytes_inside_data_push() {
        // OP_PUSH 2 bytes [0xb4, 0xd8] then OP_CHECKSIG (0xac) -> no opcode in range
        assert!(!script_uses_introspection_opcode(&[0x02, 0xb4, 0xd8, 0xac]));
    }

    #[test]
    fn ignores_standard_p2pk() {
        // OP_DATA_32 <32 bytes> OP_CHECKSIG
        let mut script = vec![0x20];
        script.extend_from_slice(&[0xb4u8; 32]); // data, not opcodes
        script.push(0xac);
        assert!(!script_uses_introspection_opcode(&script));
    }

    #[test]
    fn detects_via_redeem_script_reveal() {
        // Build a redeem script that uses OpTxOutputCount (0xb4)
        let redeem = vec![0xb4, 0xac];
        // signature script: push a signature, then push the redeem script
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // 3-byte signature push
        sig.push(redeem.len() as u8); // OP_DATA_2
        sig.extend_from_slice(&redeem);
        assert!(signature_script_reveals_introspection(&sig));
    }

    #[test]
    fn no_reveal_when_redeem_has_no_introspection() {
        // Signature script whose final push is a redeem script of just OP_CHECKSIG
        let sig = vec![0x01, 0xac];
        assert!(!signature_script_reveals_introspection(&sig));
    }

    #[test]
    fn handles_truncated_push_gracefully() {
        // OP_PUSHDATA1 claiming 200 bytes but none follow
        assert!(!script_uses_introspection_opcode(&[0x4c, 200]));
        assert!(last_push_payload(&[0x4c, 200]).is_none());
    }

    #[test]
    fn truncated_pushdata_length_prefix_is_not_misread_as_opcode() {
        // OP_PUSHDATA2 (0x4d) with only one of its two length bytes present
        // (0xb4 = OpTxOutputCount). The dangling byte must not be re-read as an
        // in-range opcode and falsely flag introspection.
        assert!(!script_uses_introspection_opcode(&[0x4d, 0xb4]));
        // OP_PUSHDATA4 (0x4e) truncated likewise.
        assert!(!script_uses_introspection_opcode(&[0x4e, 0xb4]));
    }
}
