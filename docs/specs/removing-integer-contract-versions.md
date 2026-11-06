# Removing integer contract versions

| | |
| --- | --- |
| **Status** | Step 1 done, the rest planned for the major after it |
| **Replaces** | `[MessageContract(version)]`, `contract_version`, `runax-contract-version` |
| **Related** | [JSON Schema contracts](json-schema-contracts.md), [docs/contracts.md](../contracts.md) |

An integer contract version is a label, not a check. Two services that both say "version 2" can
disagree on the shape and nothing notices, which is the whole reason schema contracts exist. Once a
contract is identified by its schema's `$id`, the integer is a second identity that nothing keeps in
step with the first — so it goes.

This is the plan for finishing that. Step 1 has shipped; the rest waits for a major, because the
integer is in the wire format and on the public API of packages already on NuGet (1.0.0 and 2.0.0).

**Which major.** Step 1 itself breaks API, so the release carrying it is 3.0 and the removal below is
4.0. Keep that straight when writing the deprecation messages: pointing a user at "removed in 3.0"
from inside 3.0 is the mistake to avoid.

## Step 1 — done

The integer has left the schema path:

- `MessageContractAttribute` has a parameterless constructor, so `[MessageContract(Schema = "…")]` is
  the schema form. `Version` is now `int?`.
- **Declaring both a `Schema` and an integer is a configuration error.** `MessageContractCache`
  rejects it the first time the type is used, on either side of the wire.
- **`x-runax-legacy-version` is gone** from the contract profile, from `ContractSchemaDocument`, from
  the linter's rules, and from the compatibility check. A leftover one in a committed file loads as a
  plain `x-` annotation rather than failing, so a 2.x schema file isn't rejected outright.
- **The integer no longer bridges to schema routing.** A consumer accepts its schema lines or its
  integer, never both, so an integer-only message doesn't reach a schema consumer and a `$id` doesn't
  reach an integer consumer.
- **`SchemaMismatch` is gone**, with `bus.ContractSchemaMismatch` and the
  `runax.messaging.contract.schema_mismatch` metric. The valve delivered a message whose `$id`
  mismatched but whose integer matched — it had no meaning left once a schema contract carries no
  integer. **Migrating a line is `Reads` now**, which is a better mechanism anyway: it is declared on
  the type that can actually read both shapes, rather than configured per bus.

What remains is the integer *mechanism*, untouched, for contracts that have not adopted schemas.

## What is left to remove

### Public API

| Member | Package | Note |
| --- | --- | --- |
| `MessageContractAttribute(int version)` and `Version` | Abstractions | The attribute keeps `Name`, `Schema`, `Reads` |
| `MessageContext.ContractVersion` | Abstractions | |
| `MessageHeaders.ContractVersion` (`runax-contract-version`) | Abstractions | Also drops out of `MessageHeaders.Reserved` |
| `UnroutableMessage.ContractVersion` | Abstractions | |
| `UnroutableMessageException.ContractVersion` and the `contractVersion` parameter | Core | Constructor signature changes |
| `IMessageConsumer.ContractVersion` | Core | Interface member; implementers break |
| `HandledContract.Version` | Core | Becomes `HandledContract(string Topic)` + `Schema` |
| `IMessageContractCatalog.Accepts(string topic, int version)` | Core | |
| `IMessageContractCatalog.Accepts(string topic, string? schema, int? version)` | Core | Collapses into `Accepts(string topic, string? schema)` |

### Wire format

- `contract_version` in the `__runax` envelope (`EnvelopeSerializer`, both directions).
- The `runax-contract-version` native broker header (`NativeHeaders`: build, parse, and the
  `HeaderPrecedence` merge).

Both are *read* as well as written, so removing them decides what happens to a message that still
carries one. See the open questions.

### Internals

- `ContractRouter`: the `wireVersion` parameter and the final `wireVersion == consumer.ContractVersion`
  branch. The `Accepts` rule reduces to "no identity declared → accept everything; otherwise the `$id`
  or the inferred lines decide".
- `MessageContractCatalog`: `Entry.Version`, and the `Version` half of `BuildEntries`/`BuildHandled`.
- `MessageConsumer<T>`: the `IMessageConsumer.ContractVersion` implementation.
- `MessageContractCache.Validate`: the both-identities check becomes "a `Schema` is required".
- `MessageConsumerHostedService`: the version in the unroutable log, the dead-letter reason, and the
  span attributes.
- `MessagingDiagnostics`: any version tag on the contract metrics.

### Docs

- **`docs/contracts.md`** (219 lines) is built around integer versioning — designing, sharing and
  evolving contracts by number. It needs rewriting around schemas, or folding into
  `docs/json-schema-contracts.md` with the integer material deleted.
- `docs/json-schema-contracts.md`: the routing table's integer rows, "From other languages", and the
  remaining "keep the integer while 2.x services route by it" advice.
- `docs/headers.md`, `docs/observability.md`, `docs/configuration.md`: `runax-contract-version` and
  any version tag.
- `README.md`: the "Contract versioning" feature bullet.
- `docs/specs/json-schema-contracts.md`: §8 phase 3 points here; its coexistence matrix has been
  corrected for the dropped bridge, but the surrounding rollout narrative still assumes one.
- A `docs/migrating-to-4.0.md`, alongside the existing `migrating-to-2.0.md`.

## Order of work

1. **Deprecate.** Mark every member in the table `[Obsolete]` with a message pointing at `Schema`, in
   a 3.x minor. Note that `src/` builds with `TreatWarningsAsErrors`, so the repo's own integer tests
   need `#pragma warning disable` or converting at the same time — do not let this land half-done, or
   the build goes red for everyone.
2. **Write `docs/migrating-to-4.0.md`** before removing anything, so the deprecation message has
   something to point at.
3. **Remove the API**, `ContractRouter`'s branch, and the catalog's half. One commit, since the
   interface member makes them inseparable.
4. **Remove the wire format**, having decided the open questions below. Transports need no changes:
   native headers go through `NativeHeaders`, so the header disappears from every transport at once.
5. **Rewrite the docs**, `docs/contracts.md` first.

## Open questions

1. **What happens to a message that still carries `contract_version`?** Three options: ignore it
   silently; ignore it but warn once per topic; or treat it as unidentified and let routing by
   validation decide (the body is all that's left). The last is the most useful and the most
   surprising. *Leaning: ignore and warn once per bus and topic, so a half-migrated fleet is visible
   rather than silent.*
2. **Do we keep reading the envelope key for one more major?** Writing it has to stop for the format
   to be clean, but reading it costs nothing and makes a mixed fleet work. *Leaning: stop writing in
   4.0, keep tolerating the key, and stop reading in 5.0.*
3. **Does `[MessageContract]` still make sense with no version?** With only `Name`, `Schema` and
   `Reads` left, it is "declare this type's contract identity". `Schema` becomes effectively required,
   since `Name` alone only renames the topic. Consider requiring `Schema` and rejecting a bare
   `[MessageContract]`.
4. **What replaces integer-only contracts for people who don't want schemas?** Today
   `[MessageContract(2)]` is a cheap way to version without authoring JSON Schema. Afterwards the
   cheapest path is a one-line schema with a required discriminator. That should be the first example
   in the migration guide, because it is the answer to "I just wanted a number".
5. **`IMessageConsumer` is public.** Removing a member breaks custom implementations. Check whether
   it should be `internal` with a public base class instead — a wider question than this change, but
   this is when it bites.

## What else needs looking at

Found while doing step 1, not caused by it:

- **`UnroutableMessageException`'s constructor** takes `(topic, contractVersion, contractSchema, …)`.
  Dropping the version from the middle is a silent source break for anyone constructing it — the
  remaining arguments still bind. Reorder or rename the parameters so it fails loudly.
- **`HandledContract` is a record with a positional `Version`.** `new HandledContract("orders", 3)`
  becomes `new HandledContract("orders")`, and the two-argument form keeps compiling against any
  future second positional parameter. Worth making `Schema` positional and `Version` removed in one
  step.
- **No test asserts the envelope's shape as a whole.** The integer's removal changes the `__runax`
  object, and nothing fails if a key is added or dropped by accident. A golden-file test over a
  serialized envelope would make wire changes visible in a diff.
- **`MessageHeaders.Reserved`** drives which native headers are stripped from user headers on
  receive. Removing a constant from it silently starts letting `runax-contract-version` through as a
  user header. Decide whether reserved names stay reserved forever.
