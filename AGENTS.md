# Agent Instructions

This is a didactic key-value store built while reading *Designing Data-Intensive Applications*. It supports two storage engines: a Bitcask-style hash-indexed engine (`kv`) and an LSM-tree engine with sorted string tables (`lsm`), selectable at startup via `--engine`. The project is educational — simplicity and clarity matter more than production-readiness.

## Task Workflow

- Tasks live in `TASKS.md` at the repo root, split into **In Progress**, **Open Tasks**, and **Closed Tasks**.
- Before starting work, read `TASKS.md` and identify the relevant task number.
- When starting a task, move its `## #N` section from **Open Tasks** to **In Progress**.
- When a task is done, move its entire `## #N` section from **In Progress** to **Closed Tasks** — never delete it.
- New tasks get the next sequential `#N` number.

## Git & PRs

- **Never push directly to `main`** — always use a dedicated branch and open a PR.
- Use **git** and the **gh CLI** for all version control and PR operations.
- Branch off `main` with the pattern `<task-number>-<short-description>` (e.g. `15-crc-checksums`).
- PR title format: `#<task-number> — <short description>`.
- PR body must include a line: `Opened via <agent>` (e.g. `Opened via Copilot`, `Opened via Claude`, `Opened via OpenCode`).
- Keep PRs focused on a single task.
- After opening a PR, add the PR link to the corresponding task in `TASKS.md`.
- Before opening the PR, move the task from **Open Tasks** to **Closed Tasks** in `TASKS.md`.
- After completing a task, always ask the user if they want to checkout back to `main`.

## Agent Role

- **Do not implement features or write production code unless the user explicitly asks.**
- The user writes the implementation — the agent assists with testing, reviewing, and running checks.
- When a task involves code changes, propose an approach and wait for the user to confirm or ask for implementation.
- Proactively write and run tests, run `cargo clippy`, and review code for correctness.

## Code Style

- Rust, built with Cargo. Source in `src/`, tests in `tests/`.
- **All tests go in the `tests/` directory** — do not use inline `#[cfg(test)]` modules in `src/`.
- Prefer hand-rolled implementations over external crates when the goal is to learn the concept.
- Follow existing patterns and module structure. New modules go in `src/`.
- Keep implementations simple. Avoid over-engineering or premature abstraction.

## Pre-commit Checklist

Before every commit, run these commands **in order** and ensure each one passes:

1. `cargo fmt` — all code must be formatted.
2. `cargo clippy -- -D warnings` — **zero warnings allowed**. If clippy reports any warnings or errors, fix them before continuing.
3. `cargo test` — all unit and integration tests must pass.

Do **not** commit or open a PR until all three pass. If a step fails, fix the issue and re-run from that step.

Before opening a PR, also check `README.md` — if the change adds or modifies CLI flags, commands, or user-visible behaviour, update the relevant section of the README.

## Project Structure

| Path | Purpose |
|------|---------|
| `src/main.rs` | TCP server, command parsing, request handling |
| `src/bin/rustikli.rs` | Interactive REPL client binary |
| `src/cli.rs` | `parse_command` — input parsing for rustikli, testable from `tests/` |
| `src/engine.rs` | `StorageEngine` trait (`get`/`set`/`delete`/`compact`/`list_keys` + `Send + Sync`) |
| `src/bffp.rs` | Binary frame fixed protocol — length-prefixed framing, encode/decode, op codes |
| `src/kvengine.rs` | Bitcask-style engine — hash index, append-only segments, hint files |
| `src/lsmengine.rs` | LSM-tree engine — memtable + sorted SSTable segments |
| `src/memtable.rs` | In-memory `BTreeMap` write buffer with size tracking and tombstones |
| `src/sstable.rs` | Sorted string table segment files with sparse index for fast lookups |
| `src/wal.rs` | Write-ahead log for LSM engine — append, replay on startup, reset after flush |
| `src/bloom.rs` | Hand-rolled Bloom filter — probabilistic membership test for fast negative lookups |
| `src/record.rs` | On-disk record format (header + CRC + key + value) |
| `src/crc.rs` | Hand-rolled CRC32 (IEEE polynomial, compile-time table) |
| `src/hash_index.rs` | In-memory hash map index (key → IndexEntry: segment + offset) |
| `src/hint.rs` | Hint file format for fast Bitcask startup (key + offset, no values) |
| `src/segment.rs` | Segment file naming, parsing, listing |
| `src/settings.rs` | CLI argument parsing, `EngineType`, `FSyncStrategy` enums |
| `src/stats.rs` | Atomic runtime counters |
| `src/worker.rs` | Background thread worker (periodic fsync, clean shutdown via `Drop`) |
| `tests/` | All tests (unit and integration) |
