//! Helpers for detecting covenant / introspection opcode usage in scripts.
//!
//! The covenant-gated opcodes added to kaspa-txscript span `OpZkPrecompile`
//! (`0xa6`) and the contiguous block `OpTxVersion..=OpBlake3WithKey`
//! (`0xb2..=0xda`). Not all of them are *introspection* opcodes, so the
//! "introspection" detector covers only the genuine introspection subset:
//!
//! - **Transaction introspection** — `OpTxVersion..=OpTxInputScriptSigLen`
//!   (`0xb2..=0xc9`), reading fields of the spending transaction.
//! - **Covenant / auth / chain introspection** — `OpAuthOutputCount`,
//!   `Op*CovenantId`, `OpCov*`, `OpChainblockSeqCommit`, up to
//!   `OpOutputAuthorizingInput` (`0xd6`).
//!
//! Deliberately *excluded* from "introspection" even though they fall in or
//! near that span: the numeric-conversion opcodes `OpNum2Bin` (`0xcd`) and
//! `OpBin2Num` (`0xce`) and the invalid/disabled opcode `OpUnknown202` (`0xca`)
//! — interior holes — and the crypto opcodes `OpCheckSigFromStack{,ECDSA}` /
//! `OpBlake3{,WithKey}` (`0xd7..=0xda`), which sit above the range max.
//! `OpZkPrecompile` (`0xa6`) sits below the range and is tracked separately, as
//! is `OpChainblockSeqCommit` (`0xd4`), which remains *inside* the introspection
//! range and is therefore an overlapping subset.
//!
//! These opcodes only ever execute inside a script body, so for a standard
//! pay-to-script-hash spend they live in the redeem script, revealed as the
//! final data push of the signature script. For a non-standard output they may
//! appear directly in the script public key.

use kaspa_txscript::opcodes::codes;

/// First byte value of the introspection opcode range (`OpTxVersion`).
pub const INTROSPECTION_OPCODE_MIN: u8 = codes::OpTxVersion;

/// Last byte value of the introspection opcode range (`OpOutputAuthorizingInput`
/// — the final covenant/chain-introspection opcode). The crypto opcodes
/// `OpCheckSigFromStack{,ECDSA}` and `OpBlake3{,WithKey}` (`0xd7..=0xda`) sit
/// above this and are not introspection.
pub const INTROSPECTION_OPCODE_MAX: u8 = codes::OpOutputAuthorizingInput;

/// Opcodes that fall inside `INTROSPECTION_OPCODE_MIN..=MAX` but are not
/// introspection, so they are excluded as interior holes: the numeric-conversion
/// opcodes `OpNum2Bin` / `OpBin2Num` and the invalid/disabled `OpUnknown202`.
const NUM2BIN_OPCODE: u8 = codes::OpNum2Bin;
const BIN2NUM_OPCODE: u8 = codes::OpBin2Num;
const UNKNOWN202_OPCODE: u8 = codes::OpUnknown202;

/// Byte value of the ZK precompile opcode (`OpZkPrecompile`). It is covenant-
/// gated like the introspection opcodes but sits below their range (`0xa6` vs
/// `0xb2..=0xd6`), so it is tracked separately.
pub const ZK_PRECOMPILE_OPCODE: u8 = codes::OpZkPrecompile;

/// Byte value of the chain-block sequencing-commitment opcode
/// (`OpChainblockSeqCommit`, `0xd4`). Unlike `OpZkPrecompile`, this opcode sits
/// *inside* the introspection range, so a script using it is counted by both the
/// introspection detector and this dedicated one (an overlapping subset).
pub const CHAINBLOCK_SEQCOMMIT_OPCODE: u8 = codes::OpChainblockSeqCommit;

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

/// Scans a single script body and returns true if any executed (non-pushed)
/// opcode satisfies `matches`. Data pushes are skipped so that bytes inside
/// pushed data are never mistaken for opcodes.
fn script_contains_opcode(script: &[u8], matches: impl Fn(u8) -> bool) -> bool {
    let mut cursor = 0;
    while cursor < script.len() {
        let op = script[cursor];
        cursor += 1;

        if matches(op) {
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

/// Scans a single script body and returns true if it contains any introspection
/// opcode — those in `INTROSPECTION_OPCODE_MIN..=MAX` excluding the interior
/// holes `OpNum2Bin` / `OpBin2Num` / `OpUnknown202`. Data pushes are skipped so
/// that bytes inside pushed data are never mistaken for opcodes.
pub fn script_uses_introspection_opcode(script: &[u8]) -> bool {
    script_contains_opcode(script, |op| {
        (INTROSPECTION_OPCODE_MIN..=INTROSPECTION_OPCODE_MAX).contains(&op)
            && op != NUM2BIN_OPCODE
            && op != BIN2NUM_OPCODE
            && op != UNKNOWN202_OPCODE
    })
}

/// Scans a single script body and returns true if it contains the ZK precompile
/// opcode (`OpZkPrecompile`). Data pushes are skipped so that bytes inside pushed
/// data are never mistaken for opcodes.
pub fn script_uses_zk_precompile_opcode(script: &[u8]) -> bool {
    script_contains_opcode(script, |op| op == ZK_PRECOMPILE_OPCODE)
}

/// Scans a single script body and returns true if it contains the chain-block
/// sequencing-commitment opcode (`OpChainblockSeqCommit`). Data pushes are
/// skipped so that bytes inside pushed data are never mistaken for opcodes.
/// Because this opcode is within the introspection range, a matching script also
/// satisfies [`script_uses_introspection_opcode`].
pub fn script_uses_chainblock_seqcommit_opcode(script: &[u8]) -> bool {
    script_contains_opcode(script, |op| op == CHAINBLOCK_SEQCOMMIT_OPCODE)
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

/// Returns true if the redeem script revealed by a signature script (its final
/// data push) uses the ZK precompile opcode (`OpZkPrecompile`).
///
/// Like [`signature_script_reveals_introspection`], this treats the final push as
/// a redeem script, so callers must gate on the spent output being P2SH.
pub fn signature_script_reveals_zk_precompile(signature_script: &[u8]) -> bool {
    match last_push_payload(signature_script) {
        Some(redeem) => script_uses_zk_precompile_opcode(redeem),
        None => false,
    }
}

/// Returns true if the redeem script revealed by a signature script (its final
/// data push) uses the chain-block sequencing-commitment opcode
/// (`OpChainblockSeqCommit`).
///
/// Like [`signature_script_reveals_introspection`], this treats the final push as
/// a redeem script, so callers must gate on the spent output being P2SH.
pub fn signature_script_reveals_chainblock_seqcommit(signature_script: &[u8]) -> bool {
    match last_push_payload(signature_script) {
        Some(redeem) => script_uses_chainblock_seqcommit_opcode(redeem),
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
        // OpOutputAuthorizingInput (0xd6), top of range
        assert!(script_uses_introspection_opcode(&[0xd6]));
        // OpChainblockSeqCommit (0xd4) is within the range
        assert!(script_uses_introspection_opcode(&[0xd4]));
    }

    #[test]
    fn excludes_crypto_opcodes_above_range() {
        // The crypto opcodes now sit above the introspection range max (0xd6)
        // and must not match: OpCheckSigFromStack (0xd7),
        // OpCheckSigFromStackECDSA (0xd8), OpBlake3 (0xd9), OpBlake3WithKey (0xda).
        assert!(!script_uses_introspection_opcode(&[0xd7]));
        assert!(!script_uses_introspection_opcode(&[0xd8]));
        assert!(!script_uses_introspection_opcode(&[0xd9]));
        assert!(!script_uses_introspection_opcode(&[0xda]));
    }

    #[test]
    fn excludes_interior_holes() {
        // OpUnknown202 (0xca, invalid), OpNum2Bin (0xcd) and OpBin2Num (0xce)
        // fall inside 0xb2..=0xd6 but are not introspection, so they are
        // excluded as interior holes...
        assert!(!script_uses_introspection_opcode(&[0xca]));
        assert!(!script_uses_introspection_opcode(&[0xcd]));
        assert!(!script_uses_introspection_opcode(&[0xce]));
        // ...while their in-range neighbors still match: OpTxInputScriptSigLen
        // (0xc9), OpAuthOutputCount (0xcb), OpAuthOutputIdx (0xcc) and
        // OpInputCovenantId (0xcf).
        assert!(script_uses_introspection_opcode(&[0xc9]));
        assert!(script_uses_introspection_opcode(&[0xcb]));
        assert!(script_uses_introspection_opcode(&[0xcc]));
        assert!(script_uses_introspection_opcode(&[0xcf]));
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
    fn detects_zk_precompile_opcode() {
        // OpZkPrecompile (0xa6) standing alone
        assert!(script_uses_zk_precompile_opcode(&[0xa6]));
        // Anything else must not match
        assert!(!script_uses_zk_precompile_opcode(&[0xa5]));
        assert!(!script_uses_zk_precompile_opcode(&[0xa7]));
    }

    #[test]
    fn zk_precompile_and_introspection_detectors_are_disjoint() {
        // An introspection-range opcode does not trigger the zk-precompile
        // detector, and vice-versa — they cover non-overlapping bytes.
        assert!(!script_uses_zk_precompile_opcode(&[0xb4])); // OpTxOutputCount
        assert!(!script_uses_introspection_opcode(&[0xa6])); // OpZkPrecompile
    }

    #[test]
    fn detects_chainblock_seqcommit_opcode() {
        // OpChainblockSeqCommit (0xd4) standing alone
        assert!(script_uses_chainblock_seqcommit_opcode(&[0xd4]));
        // Neighboring opcodes must not match
        assert!(!script_uses_chainblock_seqcommit_opcode(&[0xd3])); // OpCovOutputIdx
        assert!(!script_uses_chainblock_seqcommit_opcode(&[0xd5])); // OpOutputCovenantId
        // A zk-precompile byte must not trigger the chainblock detector
        assert!(!script_uses_chainblock_seqcommit_opcode(&[0xa6]));
    }

    #[test]
    fn chainblock_seqcommit_overlaps_introspection() {
        // OpChainblockSeqCommit (0xd4) is intentionally within the introspection
        // range, so a script using it satisfies BOTH detectors (an overlapping
        // subset, unlike the disjoint zk-precompile opcode).
        assert!(script_uses_chainblock_seqcommit_opcode(&[0xd4]));
        assert!(script_uses_introspection_opcode(&[0xd4]));
    }

    #[test]
    fn ignores_chainblock_seqcommit_byte_inside_data_push() {
        // OP_PUSH 1 byte [0xd4] then OP_CHECKSIG (0xac) -> no opcode executed
        assert!(!script_uses_chainblock_seqcommit_opcode(&[0x01, 0xd4, 0xac]));
    }

    #[test]
    fn detects_chainblock_seqcommit_via_redeem_script_reveal() {
        // Build a redeem script that uses OpChainblockSeqCommit (0xd4)
        let redeem = vec![0xd4, 0xac];
        // signature script: push a signature, then push the redeem script
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // 3-byte signature push
        sig.push(redeem.len() as u8); // OP_DATA_2
        sig.extend_from_slice(&redeem);
        assert!(signature_script_reveals_chainblock_seqcommit(&sig));
        // Being within the introspection range, the same reveal also registers
        // as introspection.
        assert!(signature_script_reveals_introspection(&sig));
    }

    #[test]
    fn ignores_zk_precompile_byte_inside_data_push() {
        // OP_PUSH 1 byte [0xa6] then OP_CHECKSIG (0xac) -> no opcode executed
        assert!(!script_uses_zk_precompile_opcode(&[0x01, 0xa6, 0xac]));
    }

    #[test]
    fn detects_zk_precompile_via_redeem_script_reveal() {
        // Build a redeem script that uses OpZkPrecompile (0xa6)
        let redeem = vec![0xa6, 0xac];
        // signature script: push a signature, then push the redeem script
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // 3-byte signature push
        sig.push(redeem.len() as u8); // OP_DATA_2
        sig.extend_from_slice(&redeem);
        assert!(signature_script_reveals_zk_precompile(&sig));
        // The same reveal must not register as introspection.
        assert!(!signature_script_reveals_introspection(&sig));
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
