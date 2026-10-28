# Runax.Messaging.Cli

The `runax-messaging` command-line tool for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Commands are grouped by area; the first is `contracts`, the CI gate for JSON Schema contracts.

## Install

```bash
dotnet tool install --global Runax.Messaging.Cli
```

Or as a local tool, pinned in the repository's tool manifest:

```bash
dotnet new tool-manifest
dotnet tool install Runax.Messaging.Cli
dotnet runax-messaging --help
```

## `contracts`

```bash
runax-messaging contracts lint schemas/
runax-messaging contracts check schemas/
```

- **`lint`** checks every `*.schema.json` against the contract profile: a draft 2020-12 `$schema`, an
  absolute `$id` that matches the file path, supported keywords only, an open root object, and unique
  `$id`s and legacy versions.
- **`check`** checks that each file is fully compatible with every earlier revision of the same `$id` in
  git history (it needs the full history, e.g. `fetch-depth: 0`), or with `--baseline <dir>`.

`--format github` prints GitHub Actions annotations. Exit codes: `0` no errors, `1` errors found,
`2` usage problems. See the [JSON Schema contracts guide](../../docs/json-schema-contracts.md#the-ci-gate).

## License

MIT
