```
██████╗ ███████╗██╗      ██████╗ ██████╗ ██╗██╗   ██╗███╗   ███╗
██╔══██╗██╔════╝██║     ██╔═══██╗██╔══██╗██║██║   ██║████╗ ████║
██║  ██║█████╗  ██║     ██║   ██║██████╔╝██║██║   ██║██╔████╔██║
██║  ██║██╔══╝  ██║     ██║   ██║██╔══██╗██║██║   ██║██║╚██╔╝██║
██████╔╝███████╗███████╗╚██████╔╝██║  ██║██║╚██████╔╝██║ ╚═╝ ██║
```
Delayed Execution Ledger Objects

The asynchronous omni-chain execution layer you didn't realize you always needed until you went back in time and saw the world before Delorium. Grim.

Bring the past... back to the future. Delorium.

<img width="640" height="480" alt="image" src="https://github.com/user-attachments/assets/255070f4-e312-482f-80c8-772b485666e8" />

## What Delorium Is

Delorium is intended to be the baseline execution layer for larger verifiable ETL pipelines that can run across any chain that Boundless supports.

At a high level, the project is about taking asynchronous offchain computation and making it reliable enough to use as chain-native infrastructure:

- prove each unit of work independently
- wait for those proofs to finalize onchain
- surface proven outputs in a form that higher-level systems can compose
- commit finalized results back onchain for downstream use

## Why This Exists

Most cross-chain data pipelines force a tradeoff between flexibility and trust. You can usually get:

- fast offchain indexing and aggregation without strong guarantees, or
- tightly scoped onchain logic that is too expensive and too limited for real ETL workloads

Delorium is aimed at the gap between those two models. The goal is to support pipelines that are:

- asynchronous by default
- composable across many proof jobs
- portable across Boundless-supported chains
- suitable as the execution substrate beneath higher-level orchestration systems

Delorium itself is not meant to define explicit map/reduce semantics. That composition layer can live above it in a separate system such as DEAN, while Delorium stays focused on execution, proving, verification, and settlement.

## Intended Shape

The initial target is a simple, verifiable pipeline:

1. take a token address and a set of accounts
2. launch a series of asynchronous proofs over historical balance snapshots
3. verify those proofs onchain
4. expose the resulting proofs and outputs to an external coordinator
5. let that coordinator aggregate them into a rolling metric such as a 7 day average
6. write the finalized value into a storage slot or another onchain sink

That same pattern should generalize into broader ETL jobs where Delorium acts as the execution substrate and DEAN or a similar system provides the higher-order workflow semantics.
