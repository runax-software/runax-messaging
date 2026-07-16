# Contributing to Runax.Messaging

Thanks for your interest in contributing! Runax.Messaging is a publish/subscribe
messaging library for .NET: a small core of abstractions (plus an in-memory provider)
with a separate library for each transport. Here's how to get started.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) (the exact version is pinned in `global.json`)
- A code editor (Rider, Visual Studio, or VS Code with the C# Dev Kit)

## Getting started

1. Fork the repository
2. Clone your fork
    ```bash
    git clone https://github.com/<your-username>/runax-messaging.git
    cd runax-messaging
    ```
3. Restore and build
    ```bash
    dotnet restore
    dotnet build
    ```
4. Run the tests
    ```bash
    dotnet test
    ```

## Project structure

```
src/
  Directory.Build.props         # Shared build/packaging settings for shippable libraries
  Runax.Messaging/              # Core: publish/subscribe abstractions + in-memory provider
  Runax.Messaging.<Transport>/  # One library per transport (e.g. Runax.Messaging.Sqs)
tests/
  Directory.Build.props         # Shared settings for test projects (xUnit v3 on MTP)
Directory.Packages.props        # Central Package Management — all NuGet versions live here
global.json                     # Pins the .NET SDK and selects the test runner
nuget.config                    # Package sources
```

Each transport lives in its own `Runax.Messaging.<Transport>` library that depends only on
the core `Runax.Messaging` package — never on another transport. Keep transport-specific SDKs
(AWS, RabbitMQ, Google Cloud, Kafka, ...) out of the core.

## Making changes

1. Create a branch from `main`
    ```bash
    git checkout -b feature/my-change
    ```
2. Make your changes
3. Ensure the project builds cleanly (`src/` treats warnings as errors)
    ```bash
    dotnet build
    ```
4. Run tests
    ```bash
    dotnet test
    ```
5. Commit using [conventional commits](https://www.conventionalcommits.org/)
    - `feat:` new feature
    - `fix:` bug fix
    - `refactor:` code change that neither fixes a bug nor adds a feature
    - `chore:` CI, deps, tooling, cleanup
    - `docs:` documentation only
6. Open a pull request against `main`

## Adding a new transport

Transports follow the `Runax.Messaging.<Provider>` naming convention (e.g.
`Runax.Messaging.Sqs`, `Runax.Messaging.Kafka`).

1. Create a new library under `src/` named `Runax.Messaging.<Provider>`. It inherits all
   common settings from `src/Directory.Build.props`, so the `.csproj` only needs its
   references.
2. Reference the core project:
    ```xml
    <ProjectReference Include="../Runax.Messaging/Runax.Messaging.csproj" />
    ```
3. Implement the core publish/subscribe abstractions for your provider.
4. Provide a dependency-injection registration extension (e.g. `AddSqs(...)`) so it can be
   wired up alongside the core registration.
5. Declare the provider SDK version in `Directory.Packages.props`, then reference it from
   your `.csproj` **without** a `Version` attribute (see below).
6. Add a matching test project under `tests/`.

## Adding a dependency

This repo uses Central Package Management, so every NuGet version is declared once.

1. Add a `<PackageVersion Include="..." Version="..." />` entry to `Directory.Packages.props`,
   in the group it fits (Core, Transports, or Tests).
2. Reference it from the project with `<PackageReference Include="..." />` — no `Version`
   attribute. Test-only packages are already provided by `tests/Directory.Build.props`.

## Code style

- Follow existing conventions in the codebase; formatting is governed by `.editorconfig`
- Use `sealed` on classes that aren't designed for inheritance
- Add XML docs to public types and members
- Keep the build warning-free — `src/` compiles with warnings-as-errors
- Keep methods focused and small

## Questions?

Open a [GitHub Discussion](https://github.com/runax-software/runax-messaging/discussions) or an issue.
