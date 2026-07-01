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
use kaspa_txscript::zk_precompiles::tags::ZkTag;
use std::ops::BitOrAssign;

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

/// ZK proof-system tag bytes, sourced from kaspa-txscript so they stay in sync
/// with consensus. `OpZkPrecompile` selects its verifier by popping a 1-byte tag
/// off the top of the stack. In the canonical consensus spend the redeem script
/// is just `[OpZkPrecompile]` and the tag is supplied as a witness push by the
/// signature script (the push immediately before the redeem-script push); a
/// non-standard script may instead push the tag inline right before the opcode.
pub const ZK_TAG_GROTH16: u8 = ZkTag::Groth16 as u8; // 0x20
pub const ZK_TAG_R0SUCCINCT: u8 = ZkTag::R0Succinct as u8; // 0x21

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

/// Returns true if `op` is an introspection opcode — one in
/// `INTROSPECTION_OPCODE_MIN..=MAX` excluding the interior holes `OpNum2Bin` /
/// `OpBin2Num` / `OpUnknown202`.
fn is_introspection_opcode(op: u8) -> bool {
    (INTROSPECTION_OPCODE_MIN..=INTROSPECTION_OPCODE_MAX).contains(&op)
        && op != NUM2BIN_OPCODE
        && op != BIN2NUM_OPCODE
        && op != UNKNOWN202_OPCODE
}

/// Scans a single script body and returns true if it contains any introspection
/// opcode — those in `INTROSPECTION_OPCODE_MIN..=MAX` excluding the interior
/// holes `OpNum2Bin` / `OpBin2Num` / `OpUnknown202`. Data pushes are skipped so
/// that bytes inside pushed data are never mistaken for opcodes.
pub fn script_uses_introspection_opcode(script: &[u8]) -> bool {
    scan_covenant_opcodes_in_script(script).introspection
}

/// Which ZK proof-system tags a script's `OpZkPrecompile` calls consume.
///
/// `unknown` is set when an `OpZkPrecompile` executes but the value on top of the
/// stack when it runs is not a recognized 1-byte tag — a non-canonical spend, or a
/// future/unrecognized tag byte. The three flags are independent: a script (or a
/// transaction aggregating several) can set more than one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ZkPrecompileTagUsage {
    pub groth16: bool,
    pub r0succinct: bool,
    pub unknown: bool,
}

impl ZkPrecompileTagUsage {
    /// True if the script uses `OpZkPrecompile` with any tag (recognized or not).
    /// Equivalent to [`script_uses_zk_precompile_opcode`].
    pub fn any(&self) -> bool {
        self.groth16 || self.r0succinct || self.unknown
    }
}

impl BitOrAssign for ZkPrecompileTagUsage {
    /// Merges another usage in, keeping any tag seen by either operand set. The
    /// single merge point stops the several accumulation sites (per output, per
    /// input, per creating-tx attribution) from drifting when a tag is added.
    fn bitor_assign(&mut self, rhs: Self) {
        self.groth16 |= rhs.groth16;
        self.r0succinct |= rhs.r0succinct;
        self.unknown |= rhs.unknown;
    }
}

/// Every covenant-opcode signal found in a single scan of one script body.
///
/// Detecting all three in one pass avoids re-walking the same script — and, for a
/// P2SH spend, re-parsing the same signature script — once per signal.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CovenantOpcodeUsage {
    /// A covenant / introspection opcode executes.
    pub introspection: bool,
    /// The chain-block sequencing-commitment opcode executes. Because it sits
    /// within the introspection range, `introspection` is also set whenever this
    /// is (an overlapping subset).
    pub chainblock_seqcommit: bool,
    /// Which ZK proof-system tags the executed `OpZkPrecompile` calls consume.
    pub zk_tags: ZkPrecompileTagUsage,
}

/// Scans one script body, classifying every executed opcode against all covenant
/// signals in a single pass. Data-push payloads are read (their bytes are never
/// mistaken for opcodes) and a truncated push stops the scan.
///
/// `stack_top` seeds the value on top of the stack when the body begins
/// executing. For a P2SH redeem script this is the item the signature script
/// pushed immediately below the redeem push — what an `OpZkPrecompile` at the
/// very start of the redeem pops as its tag. The canonical ZK-precompile spend
/// has a redeem script of just `[OpZkPrecompile]` and supplies the tag as that
/// witness push, so without the seed every real Groth16/R0Succinct spend would
/// misclassify as `unknown`. A tag pushed inside the redeem itself overrides the
/// seed, so an inline `[OpData1, tag, OpZkPrecompile]` is handled too.
fn scan_covenant_opcodes(script: &[u8], stack_top: Option<&[u8]>) -> CovenantOpcodeUsage {
    let mut usage = CovenantOpcodeUsage::default();
    let mut last_push = stack_top;
    let mut cursor = 0;

    while cursor < script.len() {
        let op = script[cursor];
        cursor += 1;

        if op == ZK_PRECOMPILE_OPCODE {
            match last_push {
                Some(&[tag]) if tag == ZK_TAG_GROTH16 => usage.zk_tags.groth16 = true,
                Some(&[tag]) if tag == ZK_TAG_R0SUCCINCT => usage.zk_tags.r0succinct = true,
                _ => usage.zk_tags.unknown = true,
            }
            // The opcode consumes the tag and pushes its own result, so a later
            // OpZkPrecompile must be preceded by its own tag.
            last_push = None;
            continue;
        }

        if op == CHAINBLOCK_SEQCOMMIT_OPCODE {
            usage.chainblock_seqcommit = true;
        }
        if is_introspection_opcode(op) {
            usage.introspection = true;
        }

        match scan_push(op, script, &mut cursor) {
            PushScan::Data(data_len) => {
                let end = cursor.saturating_add(data_len);
                if end > script.len() {
                    // Truncated push; the rest is unparseable.
                    break;
                }
                last_push = Some(&script[cursor..end]);
                cursor = end;
            }
            PushScan::Truncated => break,
            // A non-push opcode means the top of stack is no longer the value a
            // following OpZkPrecompile would read as its tag.
            PushScan::NotPush => last_push = None,
        }
    }

    usage
}

/// Scans a script body (an output script public key, or any script executed with
/// no incoming stack) for every covenant signal in one pass.
pub fn scan_covenant_opcodes_in_script(script: &[u8]) -> CovenantOpcodeUsage {
    scan_covenant_opcodes(script, None)
}

/// Scans a single script body and classifies each executed `OpZkPrecompile` by
/// the ZK tag it consumes. With no incoming stack the tag must be the 1-byte data
/// push immediately preceding the opcode; anything else classifies as `unknown`.
pub fn scan_zk_precompile_tags(script: &[u8]) -> ZkPrecompileTagUsage {
    scan_covenant_opcodes_in_script(script).zk_tags
}

/// Scans a single script body and returns true if it contains the ZK precompile
/// opcode (`OpZkPrecompile`), regardless of tag. Data pushes are skipped so that
/// bytes inside pushed data are never mistaken for opcodes.
pub fn script_uses_zk_precompile_opcode(script: &[u8]) -> bool {
    scan_zk_precompile_tags(script).any()
}

/// Scans a single script body and returns true if it contains the chain-block
/// sequencing-commitment opcode (`OpChainblockSeqCommit`). Data pushes are
/// skipped so that bytes inside pushed data are never mistaken for opcodes.
/// Because this opcode is within the introspection range, a matching script also
/// satisfies [`script_uses_introspection_opcode`].
pub fn script_uses_chainblock_seqcommit_opcode(script: &[u8]) -> bool {
    scan_covenant_opcodes_in_script(script).chainblock_seqcommit
}

/// Returns the payloads of the last two push operations in `script`, as
/// `(second_to_last, last)`. Either is `None` when fewer than that many parseable
/// pushes exist. For a P2SH signature script `last` is the redeem script and
/// `second_to_last` is the stack item the redeem script sees on top when it runs.
fn last_two_push_payloads(script: &[u8]) -> (Option<&[u8]>, Option<&[u8]>) {
    let mut cursor = 0;
    let mut prev: Option<&[u8]> = None;
    let mut last: Option<&[u8]> = None;

    while cursor < script.len() {
        let op = script[cursor];
        cursor += 1;

        match scan_push(op, script, &mut cursor) {
            PushScan::Data(data_len) => {
                let Some(end) = cursor.checked_add(data_len) else {
                    break;
                };
                if end > script.len() {
                    // Truncated push, stop parsing.
                    break;
                }
                prev = last;
                last = Some(&script[cursor..end]);
                cursor = end;
            }
            PushScan::Truncated => break,
            PushScan::NotPush => continue,
        }
    }

    (prev, last)
}

/// Returns every covenant signal in the redeem script revealed by a P2SH
/// signature script (its final push), scanned in a single pass.
///
/// The revealed redeem script is treated as executing on top of the stack the
/// signature script leaves behind, so the push immediately preceding the redeem
/// push seeds an `OpZkPrecompile`'s tag — matching the canonical spend whose
/// redeem script is just `[OpZkPrecompile]` and whose tag is a signature-script
/// witness.
///
/// This treats the final push as a redeem script, so it is only meaningful for
/// pay-to-script-hash spends; callers must gate on the spent output being P2SH.
/// Applied to a plain pay-to-pubkey spend the final push is a signature and its
/// bytes would be misread as opcodes.
pub fn signature_script_reveals_covenant_opcodes(signature_script: &[u8]) -> CovenantOpcodeUsage {
    let (stack_top, redeem) = last_two_push_payloads(signature_script);
    match redeem {
        Some(redeem) => scan_covenant_opcodes(redeem, stack_top),
        None => CovenantOpcodeUsage::default(),
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
        assert!(signature_script_reveals_covenant_opcodes(&sig).introspection);
    }

    #[test]
    fn no_reveal_when_redeem_has_no_introspection() {
        // Signature script whose final push is a redeem script of just OP_CHECKSIG
        let sig = vec![0x01, 0xac];
        assert!(!signature_script_reveals_covenant_opcodes(&sig).introspection);
    }

    #[test]
    fn handles_truncated_push_gracefully() {
        // OP_PUSHDATA1 claiming 200 bytes but none follow
        assert!(!script_uses_introspection_opcode(&[0x4c, 200]));
        assert_eq!(last_two_push_payloads(&[0x4c, 200]), (None, None));
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
        let reveal = signature_script_reveals_covenant_opcodes(&sig);
        assert!(reveal.chainblock_seqcommit);
        // Being within the introspection range, the same reveal also registers
        // as introspection.
        assert!(reveal.introspection);
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
        let reveal = signature_script_reveals_covenant_opcodes(&sig);
        assert!(reveal.zk_tags.any());
        // The same reveal must not register as introspection.
        assert!(!reveal.introspection);
    }

    #[test]
    fn classifies_zk_precompile_tags_from_inline_push() {
        // Scanned with no incoming stack, the tag must be pushed inline right
        // before the opcode: [OpData1, tag, OpZkPrecompile].
        let groth16 = scan_zk_precompile_tags(&[0x01, ZK_TAG_GROTH16, 0xa6]);
        assert_eq!(
            groth16,
            ZkPrecompileTagUsage { groth16: true, r0succinct: false, unknown: false }
        );
        assert!(groth16.any());

        let r0 = scan_zk_precompile_tags(&[0x01, ZK_TAG_R0SUCCINCT, 0xa6]);
        assert_eq!(
            r0,
            ZkPrecompileTagUsage { groth16: false, r0succinct: true, unknown: false }
        );
    }

    #[test]
    fn classifies_unrecognized_or_missing_tag_as_unknown() {
        // OpZkPrecompile with no preceding push.
        assert_eq!(
            scan_zk_precompile_tags(&[0xa6]),
            ZkPrecompileTagUsage { groth16: false, r0succinct: false, unknown: true }
        );
        // Preceding push is a recognized tag byte but multi-byte, so not a tag.
        assert_eq!(
            scan_zk_precompile_tags(&[0x02, ZK_TAG_GROTH16, 0x00, 0xa6]),
            ZkPrecompileTagUsage { groth16: false, r0succinct: false, unknown: true }
        );
        // A non-push opcode sits between the tag push and the opcode.
        assert_eq!(
            scan_zk_precompile_tags(&[0x01, ZK_TAG_GROTH16, 0xac, 0xa6]),
            ZkPrecompileTagUsage { groth16: false, r0succinct: false, unknown: true }
        );
        // Unrecognized tag byte value.
        assert_eq!(
            scan_zk_precompile_tags(&[0x01, 0x99, 0xa6]),
            ZkPrecompileTagUsage { groth16: false, r0succinct: false, unknown: true }
        );
    }

    #[test]
    fn zk_tag_byte_inside_data_push_is_not_a_zk_precompile() {
        // A 2-byte push [tag, OpZkPrecompile] followed by OpCheckSig executes no
        // OpZkPrecompile at all — the 0xa6 is pushed data, not an opcode.
        let usage = scan_zk_precompile_tags(&[0x02, ZK_TAG_GROTH16, 0xa6, 0xac]);
        assert!(!usage.any());
    }

    #[test]
    fn multiple_zk_precompiles_set_multiple_tags() {
        // Two independent tag-then-opcode sequences in one script.
        let usage = scan_zk_precompile_tags(&[
            0x01, ZK_TAG_GROTH16, 0xa6, 0x01, ZK_TAG_R0SUCCINCT, 0xa6,
        ]);
        assert!(usage.groth16 && usage.r0succinct && !usage.unknown);
    }

    #[test]
    fn signature_reveal_classifies_tag_from_canonical_witness_spend() {
        // The canonical consensus spend: redeem script is just [OpZkPrecompile]
        // and the 1-byte tag is a witness push in the signature script, sitting
        // immediately before the redeem-script push (second-to-last push overall).
        // pushes: [dummy proof] [tag] [redeem = OpZkPrecompile]
        let sig = vec![
            0x03, 0xaa, 0xbb, 0xcc, // dummy proof/signature push
            0x01, ZK_TAG_R0SUCCINCT, // tag witness push (second-to-last)
            0x01, 0xa6, // redeem script push: [OpZkPrecompile]
        ];
        let reveal = signature_script_reveals_covenant_opcodes(&sig);
        assert!(reveal.zk_tags.r0succinct && !reveal.zk_tags.groth16 && !reveal.zk_tags.unknown);
        // Aggregate detector stays consistent with the tag scan.
        assert!(reveal.zk_tags.any());
        // A ZK-precompile spend is not introspection.
        assert!(!reveal.introspection);
    }

    #[test]
    fn signature_reveal_classifies_tag_from_inline_redeem_script() {
        // A non-standard redeem that pushes the tag inline right before the
        // opcode ([OpData1, tag, OpZkPrecompile]) is classified from that inline
        // push, overriding any witness seed.
        let redeem = vec![0x01, ZK_TAG_GROTH16, 0xa6];
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // 3-byte witness push
        sig.push(redeem.len() as u8);
        sig.extend_from_slice(&redeem);
        let reveal = signature_script_reveals_covenant_opcodes(&sig);
        assert!(reveal.zk_tags.groth16 && !reveal.zk_tags.r0succinct && !reveal.zk_tags.unknown);
    }

    #[test]
    fn signature_reveal_untagged_zk_precompile_is_unknown() {
        // Redeem [OpZkPrecompile] with a multi-byte second-to-last push (a real
        // signature, not a 1-byte tag) cannot be classified -> unknown.
        let sig = vec![
            0x03, 0xaa, 0xbb, 0xcc, // multi-byte push (not a tag)
            0x01, 0xa6, // redeem script push: [OpZkPrecompile]
        ];
        let reveal = signature_script_reveals_covenant_opcodes(&sig);
        assert!(reveal.zk_tags.unknown && !reveal.zk_tags.groth16 && !reveal.zk_tags.r0succinct);
    }

    #[test]
    fn zk_tag_constants_match_upstream() {
        assert_eq!(ZK_TAG_GROTH16, 0x20);
        assert_eq!(ZK_TAG_R0SUCCINCT, 0x21);
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
