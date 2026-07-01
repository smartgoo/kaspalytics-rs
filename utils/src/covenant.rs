//! Shared per-transaction script-class / covenant tally.
//!
//! Both the daemon (live 24h metrics) and the CLI (persisted daily summary)
//! derive the same script-class breakdown and covenant figures from a
//! transaction. This module is the single source of that logic so the two paths
//! cannot drift. Callers adapt their own transaction representation into the
//! lightweight views below; everything else (classification, P2SH introspection
//! gating, covenant-id counting) lives here.
//!
//! Two deliberately distinct signals are tracked and must not be conflated:
//! - **covenant-id presence** (`is_covenant` / `spent_is_covenant`): the output
//!   carries a covenant binding / the spent output carried a covenant id. This
//!   comes from the consensus covenant fields, not from script bytes.
//! - **introspection-opcode presence** (`uses_introspection_opcode`): a script
//!   actually contains a covenant / introspection opcode, detected by scanning
//!   the redeem script revealed by a P2SH spend or a non-standard output script.

use crate::script::{
    scan_covenant_opcodes_in_script, signature_script_reveals_covenant_opcodes,
};
use kaspa_consensus_core::tx::ScriptPublicKey;
use kaspa_txscript::script_class::ScriptClass;

/// A transaction output, reduced to what the covenant tally needs.
pub struct TxOutputView<'a> {
    /// The output's script public key, classified via [`ScriptClass::from_script`].
    pub script_public_key: &'a ScriptPublicKey,
    /// Whether the output carries a covenant binding (covenant-id presence).
    pub is_covenant: bool,
}

/// A transaction input whose spent (previous) output has been resolved. Inputs
/// that cannot be resolved are simply omitted by the caller — they contribute
/// nothing to the tally rather than being counted as zero.
pub struct ResolvedInputView<'a> {
    /// The spent output's script public key (used to gate P2SH introspection).
    pub spent_script_public_key: &'a ScriptPublicKey,
    /// Whether the spent output carried a covenant id (covenant-id presence).
    pub spent_is_covenant: bool,
    /// The spending input's signature script (reveals the P2SH redeem script).
    pub signature_script: &'a [u8],
}

/// A transaction's contribution to the per-second / daily script-class and
/// covenant metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CovenantTally {
    pub output_count_pubkey: u64,
    pub output_count_pubkey_ecdsa: u64,
    pub output_count_script_hash: u64,
    pub output_count_nonstandard: u64,
    /// True if any output script, or any resolved P2SH input's revealed redeem
    /// script, uses a covenant / introspection opcode.
    pub uses_introspection_opcode: bool,
    /// True if any output script, or any resolved P2SH input's revealed redeem
    /// script, uses the ZK precompile opcode (`OpZkPrecompile`) with any tag.
    /// The per-tag flags below are overlapping subsets of this.
    pub uses_zk_precompile_opcode: bool,
    /// True if any ZK precompile use carries the Groth16 tag (`0x20`). A subset
    /// of `uses_zk_precompile_opcode`.
    pub uses_zk_precompile_groth16_opcode: bool,
    /// True if any ZK precompile use carries the R0Succinct tag (`0x21`). A subset
    /// of `uses_zk_precompile_opcode`.
    pub uses_zk_precompile_r0succinct_opcode: bool,
    /// True if any ZK precompile use carries an unrecognized / statically
    /// unresolved tag. A subset of `uses_zk_precompile_opcode`.
    pub uses_zk_precompile_unknown_tag_opcode: bool,
    /// True if any output script, or any resolved P2SH input's revealed redeem
    /// script, uses the chain-block sequencing-commitment opcode
    /// (`OpChainblockSeqCommit`). This opcode is within the introspection range,
    /// so whenever this is true `uses_introspection_opcode` is also true.
    pub uses_chainblock_seqcommit_opcode: bool,
    /// Number of P2SH outputs spent by this transaction's resolved inputs whose
    /// revealed redeem script uses a covenant / introspection opcode. Unresolved
    /// inputs are not represented, so this is a lower bound when the caller could
    /// not resolve every input.
    pub introspection_opcode_outputs_spent: u64,
    /// Number of P2SH outputs spent by this transaction's resolved inputs whose
    /// revealed redeem script uses the ZK precompile opcode (any tag). Unresolved
    /// inputs are not represented, so this is a lower bound when the caller could
    /// not resolve every input. The per-tag counts below are overlapping subsets.
    pub zk_precompile_outputs_spent: u64,
    /// Number of spent P2SH outputs whose revealed redeem script uses a Groth16-
    /// tagged ZK precompile. A subset of `zk_precompile_outputs_spent`.
    pub zk_precompile_groth16_outputs_spent: u64,
    /// Number of spent P2SH outputs whose revealed redeem script uses an
    /// R0Succinct-tagged ZK precompile. A subset of `zk_precompile_outputs_spent`.
    pub zk_precompile_r0succinct_outputs_spent: u64,
    /// Number of spent P2SH outputs whose revealed redeem script uses a ZK
    /// precompile with an unrecognized tag. A subset of
    /// `zk_precompile_outputs_spent`.
    pub zk_precompile_unknown_tag_outputs_spent: u64,
    /// Number of P2SH outputs spent by this transaction's resolved inputs whose
    /// revealed redeem script uses the chain-block sequencing-commitment opcode.
    /// Unresolved inputs are not represented, so this is a lower bound when the
    /// caller could not resolve every input.
    pub chainblock_seqcommit_outputs_spent: u64,
    /// Number of covenant-bound outputs created by this transaction.
    pub covenant_outputs_created: u64,
    /// Number of covenant-bound outputs spent by this transaction's resolved
    /// inputs. Unresolved inputs are not represented, so this is a lower bound
    /// when the caller could not resolve every input.
    pub covenant_outputs_spent: u64,
}

/// Computes a transaction's [`CovenantTally`] from its outputs and the subset of
/// its inputs whose spent outputs are resolved.
///
/// Introspection usage is monotonic: it is reported whenever a non-standard
/// output script, or any resolved P2SH input's redeem script, contains an
/// introspection opcode. Unresolved inputs can only add such evidence, never
/// remove it, so this answer never depends on inputs the caller could not
/// resolve.
pub fn tally_covenant_metrics<'a>(
    outputs: impl IntoIterator<Item = TxOutputView<'a>>,
    resolved_inputs: impl IntoIterator<Item = ResolvedInputView<'a>>,
) -> CovenantTally {
    let mut tally = CovenantTally::default();
    let mut uses_introspection = false;
    let mut zk_tags = crate::script::ZkPrecompileTagUsage::default();
    let mut uses_chainblock_seqcommit = false;

    for output in outputs {
        match ScriptClass::from_script(output.script_public_key) {
            ScriptClass::PubKey => tally.output_count_pubkey += 1,
            ScriptClass::PubKeyECDSA => tally.output_count_pubkey_ecdsa += 1,
            ScriptClass::ScriptHash => tally.output_count_script_hash += 1,
            ScriptClass::NonStandard => tally.output_count_nonstandard += 1,
        }

        if output.is_covenant {
            tally.covenant_outputs_created += 1;
        }

        // A non-standard output may carry introspection / zk-precompile opcodes
        // directly in its script public key. One scan covers every signal.
        let output_usage = scan_covenant_opcodes_in_script(output.script_public_key.script());
        uses_introspection |= output_usage.introspection;
        zk_tags |= output_usage.zk_tags;
        uses_chainblock_seqcommit |= output_usage.chainblock_seqcommit;
    }

    for input in resolved_inputs {
        if input.spent_is_covenant {
            tally.covenant_outputs_spent += 1;
        }

        // Covenant scripts are committed via P2SH, so introspection opcodes are
        // only revealed in the redeem script of a pay-to-script-hash spend.
        // Gating on the spent output's class avoids misreading signature bytes
        // as opcodes on a non-P2SH spend.
        if ScriptClass::from_script(input.spent_script_public_key) == ScriptClass::ScriptHash {
            // One scan of the revealed redeem script covers every signal.
            let reveal = signature_script_reveals_covenant_opcodes(input.signature_script);
            if reveal.introspection {
                uses_introspection = true;
                tally.introspection_opcode_outputs_spent += 1;
            }
            let input_zk_tags = reveal.zk_tags;
            zk_tags |= input_zk_tags;
            if input_zk_tags.any() {
                tally.zk_precompile_outputs_spent += 1;
            }
            if input_zk_tags.groth16 {
                tally.zk_precompile_groth16_outputs_spent += 1;
            }
            if input_zk_tags.r0succinct {
                tally.zk_precompile_r0succinct_outputs_spent += 1;
            }
            if input_zk_tags.unknown {
                tally.zk_precompile_unknown_tag_outputs_spent += 1;
            }
            if reveal.chainblock_seqcommit {
                uses_chainblock_seqcommit = true;
                tally.chainblock_seqcommit_outputs_spent += 1;
            }
        }
    }

    tally.uses_introspection_opcode = uses_introspection;
    tally.uses_zk_precompile_opcode = zk_tags.any();
    tally.uses_zk_precompile_groth16_opcode = zk_tags.groth16;
    tally.uses_zk_precompile_r0succinct_opcode = zk_tags.r0succinct;
    tally.uses_zk_precompile_unknown_tag_opcode = zk_tags.unknown;
    tally.uses_chainblock_seqcommit_opcode = uses_chainblock_seqcommit;
    tally
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaspa_consensus_core::tx::ScriptPublicKey;
    use kaspa_txscript::opcodes::codes;
    use kaspa_txscript::MAX_SCRIPT_PUBLIC_KEY_VERSION;

    const VER: u16 = MAX_SCRIPT_PUBLIC_KEY_VERSION;

    fn spk(script: Vec<u8>) -> ScriptPublicKey {
        ScriptPublicKey::from_vec(VER, script)
    }

    /// `OpData32 <32 bytes> OpCheckSig` — standard pay-to-pubkey.
    fn p2pk() -> ScriptPublicKey {
        let mut s = vec![codes::OpData32];
        s.extend_from_slice(&[0x11u8; 32]);
        s.push(codes::OpCheckSig);
        spk(s)
    }

    /// `OpData33 <33 bytes> OpCheckSigECDSA` — standard ECDSA pay-to-pubkey.
    fn p2pk_ecdsa() -> ScriptPublicKey {
        let mut s = vec![codes::OpData33];
        s.extend_from_slice(&[0x22u8; 33]);
        s.push(codes::OpCheckSigECDSA);
        spk(s)
    }

    /// `OpBlake2b OpData32 <32 bytes> OpEqual` — standard pay-to-script-hash.
    fn p2sh() -> ScriptPublicKey {
        let mut s = vec![codes::OpBlake2b, codes::OpData32];
        s.extend_from_slice(&[0x33u8; 32]);
        s.push(codes::OpEqual);
        spk(s)
    }

    /// A non-standard script that contains no covenant opcode.
    fn nonstandard_plain() -> ScriptPublicKey {
        spk(vec![codes::OpFalse])
    }

    /// A signature script whose final push is a redeem script using a covenant
    /// opcode (OpTxOutputCount). Mirrors a real P2SH covenant reveal.
    fn sig_revealing_introspection() -> Vec<u8> {
        let redeem = vec![codes::OpTxOutputCount, codes::OpCheckSig];
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // dummy signature push
        sig.push(redeem.len() as u8); // OP_DATA_2
        sig.extend_from_slice(&redeem);
        sig
    }

    /// A signature script whose final push is a redeem script using the ZK
    /// precompile opcode. Mirrors a real P2SH OpZkPrecompile reveal.
    fn sig_revealing_zk_precompile() -> Vec<u8> {
        let redeem = vec![codes::OpZkPrecompile, codes::OpCheckSig];
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // dummy signature push
        sig.push(redeem.len() as u8); // OP_DATA_2
        sig.extend_from_slice(&redeem);
        sig
    }

    /// A signature script for the canonical consensus ZK-precompile spend: the
    /// redeem script is just `[OpZkPrecompile]` and the 1-byte tag is a witness
    /// push in the signature script, immediately before the redeem push. Mirrors
    /// `pay_to_script_hash_signature_script(redeem, prefix)` where the prefix ends
    /// in `add_data(&[tag])`.
    fn sig_revealing_zk_tag(tag: u8) -> Vec<u8> {
        vec![
            0x03, 0xaa, 0xbb, 0xcc, // dummy proof/signature push
            0x01, tag, // tag witness push (second-to-last)
            0x01, codes::OpZkPrecompile, // redeem script push: [OpZkPrecompile]
        ]
    }

    /// A signature script for a non-standard spend that pushes the tag inline in
    /// the redeem script itself (`[OpData1, tag, OpZkPrecompile]`).
    fn sig_revealing_zk_tag_inline(tag: u8) -> Vec<u8> {
        let redeem = vec![0x01, tag, codes::OpZkPrecompile];
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // dummy witness push
        sig.push(redeem.len() as u8);
        sig.extend_from_slice(&redeem);
        sig
    }

    /// A signature script whose final push is a redeem script using the
    /// chain-block sequencing-commitment opcode. Mirrors a real P2SH
    /// OpChainblockSeqCommit reveal.
    fn sig_revealing_chainblock_seqcommit() -> Vec<u8> {
        let redeem = vec![codes::OpChainblockSeqCommit, codes::OpCheckSig];
        let mut sig = vec![0x03, 0xaa, 0xbb, 0xcc]; // dummy signature push
        sig.push(redeem.len() as u8); // OP_DATA_2
        sig.extend_from_slice(&redeem);
        sig
    }

    #[test]
    fn classifies_each_output_script_class() {
        let (a, b, c, d) = (p2pk(), p2pk_ecdsa(), p2sh(), nonstandard_plain());
        let outputs = vec![
            TxOutputView { script_public_key: &a, is_covenant: false },
            TxOutputView { script_public_key: &b, is_covenant: false },
            TxOutputView { script_public_key: &c, is_covenant: false },
            TxOutputView { script_public_key: &d, is_covenant: false },
        ];

        let tally = tally_covenant_metrics(outputs, std::iter::empty());

        assert_eq!(tally.output_count_pubkey, 1);
        assert_eq!(tally.output_count_pubkey_ecdsa, 1);
        assert_eq!(tally.output_count_script_hash, 1);
        assert_eq!(tally.output_count_nonstandard, 1);
        assert_eq!(tally.covenant_outputs_created, 0);
        assert_eq!(tally.covenant_outputs_spent, 0);
        assert!(!tally.uses_introspection_opcode);
    }

    #[test]
    fn counts_covenant_id_presence_independent_of_opcodes() {
        // Covenant-id presence is a flag on the output/input, NOT derived from
        // script bytes — a plain P2PK output flagged covenant still counts, and
        // it must not set uses_introspection_opcode.
        let out = p2pk();
        let spent = p2pk();
        let sig = vec![0x01, codes::OpCheckSig];

        let tally = tally_covenant_metrics(
            vec![TxOutputView { script_public_key: &out, is_covenant: true }],
            vec![ResolvedInputView {
                spent_script_public_key: &spent,
                spent_is_covenant: true,
                signature_script: &sig,
            }],
        );

        assert_eq!(tally.covenant_outputs_created, 1);
        assert_eq!(tally.covenant_outputs_spent, 1);
        assert!(!tally.uses_introspection_opcode);
    }

    #[test]
    fn detects_introspection_in_nonstandard_output_script() {
        // A non-standard output whose script public key directly contains a
        // covenant opcode.
        let out = spk(vec![codes::OpTxOutputCount, codes::OpCheckSig]);

        let tally = tally_covenant_metrics(
            vec![TxOutputView { script_public_key: &out, is_covenant: false }],
            std::iter::empty(),
        );

        assert_eq!(tally.output_count_nonstandard, 1);
        assert!(tally.uses_introspection_opcode);
        // Detected in an output script, not a spend, so nothing is "spent".
        assert_eq!(tally.introspection_opcode_outputs_spent, 0);
    }

    #[test]
    fn detects_introspection_revealed_by_p2sh_input() {
        let spent = p2sh();
        let sig = sig_revealing_introspection();

        let tally = tally_covenant_metrics(
            std::iter::empty(),
            vec![ResolvedInputView {
                spent_script_public_key: &spent,
                spent_is_covenant: false,
                signature_script: &sig,
            }],
        );

        assert!(tally.uses_introspection_opcode);
        assert_eq!(tally.introspection_opcode_outputs_spent, 1);
    }

    #[test]
    fn detects_zk_precompile_revealed_by_p2sh_input() {
        // A P2SH input whose redeem script uses OpZkPrecompile sets the
        // zk-precompile flag without setting the introspection flag (the opcode
        // sits below the introspection range). This redeem carries no tag push,
        // so it classifies as the unknown-tag bucket.
        let spent = p2sh();
        let sig = sig_revealing_zk_precompile();

        let tally = tally_covenant_metrics(
            std::iter::empty(),
            vec![ResolvedInputView {
                spent_script_public_key: &spent,
                spent_is_covenant: false,
                signature_script: &sig,
            }],
        );

        assert!(tally.uses_zk_precompile_opcode);
        assert!(!tally.uses_introspection_opcode);
        assert_eq!(tally.zk_precompile_outputs_spent, 1);
        assert_eq!(tally.introspection_opcode_outputs_spent, 0);
        // Untagged reveal → unknown-tag bucket, not groth16/r0succinct.
        assert!(tally.uses_zk_precompile_unknown_tag_opcode);
        assert!(!tally.uses_zk_precompile_groth16_opcode);
        assert!(!tally.uses_zk_precompile_r0succinct_opcode);
        assert_eq!(tally.zk_precompile_unknown_tag_outputs_spent, 1);
    }

    #[test]
    fn classifies_zk_precompile_tag_buckets_from_p2sh_reveals() {
        use crate::script::{ZK_TAG_GROTH16, ZK_TAG_R0SUCCINCT};
        let spent = p2sh();
        let groth16_sig = sig_revealing_zk_tag(ZK_TAG_GROTH16);
        let r0_sig = sig_revealing_zk_tag(ZK_TAG_R0SUCCINCT);

        let tally = tally_covenant_metrics(
            std::iter::empty(),
            vec![
                ResolvedInputView {
                    spent_script_public_key: &spent,
                    spent_is_covenant: false,
                    signature_script: &groth16_sig,
                },
                ResolvedInputView {
                    spent_script_public_key: &spent,
                    spent_is_covenant: false,
                    signature_script: &r0_sig,
                },
            ],
        );

        // Aggregate counts both spent outputs once; each tag bucket counts its own.
        assert!(tally.uses_zk_precompile_opcode);
        assert_eq!(tally.zk_precompile_outputs_spent, 2);
        assert!(tally.uses_zk_precompile_groth16_opcode);
        assert_eq!(tally.zk_precompile_groth16_outputs_spent, 1);
        assert!(tally.uses_zk_precompile_r0succinct_opcode);
        assert_eq!(tally.zk_precompile_r0succinct_outputs_spent, 1);
        // No untagged reveals here.
        assert!(!tally.uses_zk_precompile_unknown_tag_opcode);
        assert_eq!(tally.zk_precompile_unknown_tag_outputs_spent, 0);
        // ZK precompile is disjoint from introspection.
        assert!(!tally.uses_introspection_opcode);
    }

    #[test]
    fn classifies_zk_precompile_tag_from_inline_redeem_reveal() {
        // A non-standard redeem that carries the tag inline still classifies.
        use crate::script::ZK_TAG_GROTH16;
        let spent = p2sh();
        let sig = sig_revealing_zk_tag_inline(ZK_TAG_GROTH16);

        let tally = tally_covenant_metrics(
            std::iter::empty(),
            vec![ResolvedInputView {
                spent_script_public_key: &spent,
                spent_is_covenant: false,
                signature_script: &sig,
            }],
        );

        assert!(tally.uses_zk_precompile_groth16_opcode);
        assert_eq!(tally.zk_precompile_groth16_outputs_spent, 1);
        assert!(!tally.uses_zk_precompile_unknown_tag_opcode);
    }

    #[test]
    fn detects_chainblock_seqcommit_revealed_by_p2sh_input() {
        // A P2SH input whose redeem script uses OpChainblockSeqCommit sets the
        // chainblock flag AND, because that opcode is within the introspection
        // range, the introspection flag too (an overlapping subset).
        let spent = p2sh();
        let sig = sig_revealing_chainblock_seqcommit();

        let tally = tally_covenant_metrics(
            std::iter::empty(),
            vec![ResolvedInputView {
                spent_script_public_key: &spent,
                spent_is_covenant: false,
                signature_script: &sig,
            }],
        );

        assert!(tally.uses_chainblock_seqcommit_opcode);
        assert!(tally.uses_introspection_opcode);
        assert!(!tally.uses_zk_precompile_opcode);
        assert_eq!(tally.chainblock_seqcommit_outputs_spent, 1);
        assert_eq!(tally.introspection_opcode_outputs_spent, 1);
    }

    #[test]
    fn detects_chainblock_seqcommit_in_nonstandard_output_script() {
        let out = spk(vec![codes::OpChainblockSeqCommit, codes::OpCheckSig]);

        let tally = tally_covenant_metrics(
            vec![TxOutputView { script_public_key: &out, is_covenant: false }],
            std::iter::empty(),
        );

        assert_eq!(tally.output_count_nonstandard, 1);
        assert!(tally.uses_chainblock_seqcommit_opcode);
        assert!(tally.uses_introspection_opcode);
        assert_eq!(tally.chainblock_seqcommit_outputs_spent, 0);
    }

    #[test]
    fn detects_zk_precompile_in_nonstandard_output_script() {
        let out = spk(vec![codes::OpZkPrecompile, codes::OpCheckSig]);

        let tally = tally_covenant_metrics(
            vec![TxOutputView { script_public_key: &out, is_covenant: false }],
            std::iter::empty(),
        );

        assert_eq!(tally.output_count_nonstandard, 1);
        assert!(tally.uses_zk_precompile_opcode);
        // Untagged output-script use → unknown-tag bucket.
        assert!(tally.uses_zk_precompile_unknown_tag_opcode);
        assert!(!tally.uses_zk_precompile_groth16_opcode);
        assert!(!tally.uses_introspection_opcode);
        assert_eq!(tally.zk_precompile_outputs_spent, 0);
    }

    #[test]
    fn ignores_introspection_reveal_when_spend_is_not_p2sh() {
        // Same revealing signature script, but the spent output is P2PK, not
        // P2SH — its final push is a signature, so its bytes must not be read as
        // opcodes.
        let spent = p2pk();
        let sig = sig_revealing_introspection();

        let tally = tally_covenant_metrics(
            std::iter::empty(),
            vec![ResolvedInputView {
                spent_script_public_key: &spent,
                spent_is_covenant: false,
                signature_script: &sig,
            }],
        );

        assert!(!tally.uses_introspection_opcode);
        assert_eq!(tally.introspection_opcode_outputs_spent, 0);
    }

    #[test]
    fn introspection_is_monotonic_across_a_mixed_transaction() {
        // One resolved P2SH input reveals introspection; the presence of other
        // ordinary inputs/outputs does not suppress the flag.
        let out = p2pk();
        let spent_p2sh = p2sh();
        let spent_p2pk = p2pk();
        let revealing = sig_revealing_introspection();
        let plain_sig = vec![0x01, codes::OpCheckSig];

        let tally = tally_covenant_metrics(
            vec![TxOutputView { script_public_key: &out, is_covenant: false }],
            vec![
                ResolvedInputView {
                    spent_script_public_key: &spent_p2pk,
                    spent_is_covenant: false,
                    signature_script: &plain_sig,
                },
                ResolvedInputView {
                    spent_script_public_key: &spent_p2sh,
                    spent_is_covenant: false,
                    signature_script: &revealing,
                },
            ],
        );

        assert!(tally.uses_introspection_opcode);
        assert_eq!(tally.introspection_opcode_outputs_spent, 1);
        assert_eq!(tally.output_count_pubkey, 1);
    }

    #[test]
    fn empty_transaction_yields_default_tally() {
        let tally = tally_covenant_metrics(std::iter::empty(), std::iter::empty());
        assert_eq!(tally, CovenantTally::default());
    }
}
