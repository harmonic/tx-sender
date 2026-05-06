# harmonic-tx-sender

Leader-targeted Solana transaction sender.

## Build

```sh
cargo build --release
```

Binary lands at `./target/release/harmonic-tx-sender`.

## Setup

### 1. Generate and fund a keypairs directory

The sender rotates through a directory of payer keypairs.
Each keypair pays its own base fees and priority fees.

Generate:

```sh
mkdir keypairs
for i in $(seq 1 $NUM_KEYPAIRS); do
  solana-keygen new -s --no-bip39-passphrase -o "keypairs/k${i}.json"
done
```

Fund — either by airdrop on testnet:

```sh
for f in keypairs/*.json; do
  solana airdrop -k "$f" 1 -u testnet
done
```

or by transferring from an already-funded keypair:

```sh
FUNDER=/path/to/funder.json
for f in keypairs/*.json; do
  solana transfer "$(solana-keygen pubkey "$f")" 1 \
    -u testnet --from "$FUNDER" --fee-payer "$FUNDER" --allow-unfunded-recipient
done
```

Each tx pays a 5000-lamport base fee plus a per-CU priority fee that runs from a
few thousand up to ~15K lamports depending on the recipe.

### 2. Write a config

Edit or copy [`config/example.toml`](config/example.toml). At minimum, set:

- `rpc_url` — any Solana RPC for blockhash + leader-schedule queries
- `keypairs` — path to the directory you just populated
- `identity` — base58 pubkey of the validator you want to target
- `target_tps` — sustained tx rate

and exactly one of the following delivery paths:

- `tpu_addr` — direct QUIC to a specific validator TPU socket
- `block_engine_url` — Harmonic bundles
- `jito_url` — Jito bundles via Jito's public block engine

### 3. (Block engine only) Whitelist your searcher key

Using `block_engine_url` requires that the keypair authenticating with the block
engine is whitelisted. By default that's the first keypair in your `keypairs/`
directory; override with `harmonic_bundle_keypair` to point at a dedicated
searcher key.

Get the pubkey:

```sh
solana-keygen pubkey keypairs/k1.json
```

Then **contact the Harmonic team** with that pubkey to be whitelisted. Without
whitelisting, auth fails immediately with `Pubkey not whitelisted for role`.

## Run

```sh
./target/release/harmonic-tx-sender <config.toml>
```

The process runs until killed with Ctrl-C.
