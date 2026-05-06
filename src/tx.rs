//! Transaction generation: 70/30 transfer/sigverify mix, sampled per tx.
//! Sigverify count varies uniformly over [2, 8], spanning ~7K–27K CU.

use anyhow::Result;
use rand::Rng;
use rand::rngs::SmallRng;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_hash::Hash;
use solana_instruction::Instruction;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_system_interface::instruction as system_instruction;
use solana_transaction::Transaction;

/// Ed25519 precompile program id.
const ED25519_PROGRAM_ID: Pubkey = Pubkey::new_from_array([
    3, 125, 70, 214, 124, 147, 251, 190, 229, 151, 24, 221, 28, 171, 26, 71, 91, 216, 229, 77, 228,
    176, 140, 163, 180, 134, 212, 157, 106, 216, 82, 177,
]);

/// Probability (out of 100) of generating a sigverify tx instead of a transfer.
const SIGVERIFY_PCT: u32 = 30;

/// Priority fee per CU set on every recipe tx, in micro-lamports. Sized to
/// clear block engines' typical 300k µlamports/CU avg-price admission
/// threshold with margin. We also stamp the same value on tip txs so the
/// bundle's weighted-average CU price stays uniform.
const PRIORITY_FEE_MICROLAMPORTS: u64 = 500_000;

pub struct TxBuilder {
    payers: Vec<Keypair>,
    nonce: u64,
}

impl TxBuilder {
    pub fn new(payers: Vec<Keypair>) -> Result<Self> {
        anyhow::ensure!(!payers.is_empty(), "no payers");
        Ok(Self { payers, nonce: 0 })
    }

    /// Build the next signed recipe transaction. Returns (tx, payer_idx, nonce);
    /// caller threads `nonce` into `build_tip` so the tip tx is also unique.
    pub fn next(&mut self, blockhash: Hash, rng: &mut SmallRng) -> (Transaction, usize, u64) {
        let n = self.nonce;
        self.nonce = self.nonce.wrapping_add(1);
        // Round-robin payer selection. The fee-payer is a writable account, so
        // distinct payers give the validator's bank scheduler distinct lock
        // lanes and parallelize execution across the spam stream.
        let payer_idx = (n as usize) % self.payers.len();
        let payer = &self.payers[payer_idx];
        let ixs = build_ixs(payer.pubkey(), n, rng);
        let tx =
            Transaction::new_signed_with_payer(&ixs, Some(&payer.pubkey()), &[payer], blockhash);
        (tx, payer_idx, n)
    }

    /// Build a tip transfer signed by the same payer that signed the recipe.
    /// `nonce` is mixed into the lamport amount so successive tips from the
    /// same payer to the same tip account in the same blockhash don't dedup.
    /// We pin the tip's CU limit and price so it doesn't drag the bundle's
    /// weighted-average CU price down below the block engine's admission floor.
    pub fn build_tip(
        &self,
        payer_idx: usize,
        blockhash: Hash,
        tip_account: &Pubkey,
        base_lamports: u64,
        nonce: u64,
    ) -> Transaction {
        let payer = &self.payers[payer_idx];
        let lamports = base_lamports + (nonce & 0xff);
        Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(1_000),
                ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS),
                system_instruction::transfer(&payer.pubkey(), tip_account, lamports),
            ],
            Some(&payer.pubkey()),
            &[payer],
            blockhash,
        )
    }
}

fn build_ixs(payer: Pubkey, nonce: u64, rng: &mut SmallRng) -> Vec<Instruction> {
    if rng.gen_range(0..100) < SIGVERIFY_PCT {
        let count = rng.gen_range(2..=8);
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(count as u32 * 3_300 + 500),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS),
            sigverify_ix(count, nonce),
        ]
    } else {
        // 0-lamport transfer to a nonce-derived dummy destination. The dest
        // varies per tx so the message bytes differ — without this, same-payer
        // same-blockhash transfers serialize to byte-identical txs and the
        // validator dedups them as "already processed".
        let mut dest = [0u8; 32];
        dest[..8].copy_from_slice(&nonce.to_le_bytes());
        dest[8] = 0xfa;
        let dest = Pubkey::new_from_array(dest);
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(1_000),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS),
            system_instruction::transfer(&payer, &dest, 0),
        ]
    }
}

/// Ed25519 precompile instruction. Layout per Solana docs:
///   1 byte    num_signatures
///   1 byte    padding
///   14 bytes  per-sig offsets
///   64 bytes  signature
///   32 bytes  pubkey
///   8 bytes   message (per-tx nonce)
///
/// Bogus signatures fail after sigverify completes — the work we want to
/// measure is already done.
fn sigverify_ix(count: usize, nonce: u64) -> Instruction {
    const SIG_LEN: usize = 64;
    const PUBKEY_LEN: usize = 32;
    const MSG_LEN: usize = 8;
    const OFFSETS_LEN: usize = 14;

    let header = 2;
    let per_sig = OFFSETS_LEN + SIG_LEN + PUBKEY_LEN + MSG_LEN;
    let mut data = vec![0u8; header + count * per_sig];
    data[0] = count as u8;

    let nonce_bytes = nonce.to_le_bytes();
    let mut cursor = header + count * OFFSETS_LEN;
    for i in 0..count {
        let off = header + i * OFFSETS_LEN;
        let sig_off = cursor;
        let pk_off = sig_off + SIG_LEN;
        let msg_off = pk_off + PUBKEY_LEN;
        data[off..off + 2].copy_from_slice(&(sig_off as u16).to_le_bytes());
        data[off + 2..off + 4].copy_from_slice(&u16::MAX.to_le_bytes());
        data[off + 4..off + 6].copy_from_slice(&(pk_off as u16).to_le_bytes());
        data[off + 6..off + 8].copy_from_slice(&u16::MAX.to_le_bytes());
        data[off + 8..off + 10].copy_from_slice(&(msg_off as u16).to_le_bytes());
        data[off + 10..off + 12].copy_from_slice(&(MSG_LEN as u16).to_le_bytes());
        data[off + 12..off + 14].copy_from_slice(&u16::MAX.to_le_bytes());
        data[msg_off..msg_off + MSG_LEN].copy_from_slice(&nonce_bytes);
        cursor += SIG_LEN + PUBKEY_LEN + MSG_LEN;
    }

    Instruction {
        program_id: ED25519_PROGRAM_ID,
        accounts: vec![],
        data,
    }
}

pub fn load_keypairs(dir: &std::path::Path) -> Result<Vec<Keypair>> {
    anyhow::ensure!(
        dir.is_dir(),
        "keypairs path {} is not a directory",
        dir.display()
    );
    let mut paths: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    paths.sort();
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        out.push(
            solana_keypair::read_keypair_file(&p)
                .map_err(|e| anyhow::anyhow!("read {}: {e}", p.display()))?,
        );
    }
    anyhow::ensure!(!out.is_empty(), "no *.json keypairs in {}", dir.display());
    println!("loaded {} payers from {}", out.len(), dir.display());
    Ok(out)
}
