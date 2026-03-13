# Derivative Pricing Example

This example frames Delorium as a system for bootstrapping an onchain price feed for an asset that is not already covered by Chainlink.

The concrete example is a Boundless price feed on Base derived from a Uniswap v4 pool. The point is not that Delorium is "a Chainlink replacement." The point is that Delorium should make it easy to specify a computational graph, prove the graph's outputs with Boundless, and then expose the result through a familiar adapter surface such as a Chainlink-compatible feed contract.

## Why This Matters

A major obstacle to exploring new onchain derivatives is the difficulty of producing a trustworthy price surface.

Writing the derivative logic is usually the easy part. The harder part is:

- choosing and normalizing data sources
- dealing with historical data
- protecting against flash manipulation
- updating only when economically justified
- exposing the result in a format existing contracts can already consume

Delorium is meant to reduce that complexity by making price computation a graph authoring problem rather than a custom oracle implementation problem.

## Example Goal

Build a Chainlink-compatible feed for Boundless on Base using a Uniswap v4 pool as the primary source.

The feed should:

- use explicit historical checkpoints
- support manipulation resistance through historical context
- avoid paying for a fresh full proof on every small downstream write
- expose a standard feed interface to consumers

## Why Not Just Use a Quote

The Uniswap v4 quoter is useful offchain, but a single quote is not a robust price surface by itself.

A usable derivative feed needs more than a proved quote:

- historical context
- update policy
- flash protection
- source normalization
- deterministic publication rules

So the real artifact Delorium should produce is not "one proved quote." It is a proved, reusable batch of observations and derived outputs that an adapter can turn into a price feed.

## Graph Model

For this example, the Delorium graph has these components:

- trigger nodes
- source nodes
- mapper nodes
- reducer nodes
- destination nodes
- adapters

The important architectural idea is that mappers and reducers define proof boundaries.

- a mapper defines independently provable units of work
- a reducer defines how those proven units are combined into a final result

## Example Graph

### Trigger

The graph is triggered when either of these conditions is met:

- the currently published feed is stale beyond a configured threshold
- the observed deviation from the currently published value exceeds a configured threshold

This maps cleanly to Boundless smart-contract requestors.

Anyone should be able to request a new proof, but the requestor contract only authorizes payment if the trigger policy is satisfied.

### Sources

The source layer reads from:

- the relevant Boundless Uniswap v4 pool on Base
- explicit historical checkpoints over a configured window

The checkpoints are part of the graph spec. Example:

- every 5 minutes
- for the last 24 hours
- yielding 288 checkpoints

This is important because derivative and oracle computations usually need explicit historical fetch points, not an implicit notion of "some history."

### Mapper

The mapper fans out the historical checkpoint queries into independently provable jobs.

Examples:

- one proof job per checkpoint
- one proof job per checkpoint group
- one proof job per source if multiple markets are involved

For the MVP, the simplest mapper is:

- one historical checkpoint per proof partition

Each mapper output should contain the normalized pool-state-derived observation needed by later reducers.

### Reducer

The reducer combines proven mapper outputs into a final pricing artifact.

Possible reducer strategies include:

- TWAP
- median
- weighted mean
- clipped mean
- quorum or fallback logic

For this example, the reducer computes a TWAP-like price over the checkpoint window.

### Destination

The destination publishes a price artifact that downstream contracts can consume.

For the MVP, the destination is not "raw historical data." It is:

- a latest finalized price
- metadata about the computation window
- a commitment to the underlying batch

### Adapters

Adapters encapsulate protocol-specific behavior.

This is important because authors of Delorium graphs should not have to write smart contracts directly.

In this example:

- source adapters know how to read or reconstruct data from Uniswap v4 state
- destination adapters know how to expose the final result through a Chainlink-compatible interface

## Merklized Compute

Delorium should merklize graph outputs.

This is one of the most important practical design choices because we do not want to redo expensive proofs for every small publication step.

The intended flow is:

1. Delorium executes a batch of mapper jobs and reducer logic offchain.
2. Boundless proves that computation.
3. The run output is committed as a Merkle root.
4. Later writes or publications provide leaf data plus inclusion proofs.
5. The adapter accepts those writes without requiring the full graph proof to be repeated.

This turns the system into:

- one expensive proof per batch
- many cheaper inclusion-based materializations later

That is a much better cost surface than proving every individual publication from scratch.

## Materialization Strategy

The full intermediate data set does not need to live entirely in contract storage.

The likely split is:

- onchain:
  - latest published price
  - computation metadata
  - Merkle root for the latest batch
  - possibly a small ring buffer of recent finalized outputs
- offchain or DA:
  - full checkpoint data
  - mapper outputs
  - reducer support data
  - inclusion proof material

This means Delorium should treat historical checkpoints as an explicit graph concept, along with a retention or materialization policy.

For example:

- checkpoint schedule: every 5 minutes
- retention window: last 24 hours
- onchain materialization: latest value plus batch root
- optional local history: bounded ring buffer for recent finalized outputs

## Request Policy

Boundless smart-contract requestors are a good fit for update control.

The requestor policy can decide whether a new graph run is worth paying for. For example:

- allow a request if the feed is older than 5 minutes
- allow a request if deviation exceeds 50 bps
- allow a request if either condition is true

This is better than forcing updates on a fixed cadence regardless of market conditions.

## What The Adapter Actually Does

The adapter is not just a dumb sink.

A good adapter can synthesize a useful price surface from solid historical inputs plus update policy.

For this example, the adapter should:

- accept finalized Delorium outputs
- verify the relevant commitment or inclusion proof
- expose a Chainlink-compatible read surface
- enforce freshness and publication policy

Over time, the adapter might also maintain:

- a small ring buffer of recent published outputs
- local metadata for consumer safety checks

## MVP Shape

The MVP for this derivative-pricing example should be:

1. define a graph for a Boundless/Base price feed using a Uniswap v4 source
2. define explicit historical checkpoints
3. run mapper jobs over those checkpoints
4. reduce them into a TWAP-style output
5. commit the run as a Merkleized batch
6. publish the latest value through a Chainlink-compatible adapter
7. gate future updates behind a Boundless requestor with staleness and deviation triggers

## Why This Is Useful For Derivatives

If Delorium can make this flow easy, it lowers one of the biggest barriers to exploring new onchain derivatives:

- creating a trustworthy, manipulaton-aware price surface for assets that do not already have production-grade oracle support

That is the real value of the system. It is not just proving arbitrary computations. It is making custom oracle and risk pipelines cheap enough to specify, prove, and consume.
