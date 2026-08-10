# Agent instructions

Guidance for automated coding agents working in this repository.

## Changelog updates

When a change is **user-visible or otherwise significant** (behavior, API/protocol,
defaults, security, packaging, deployment, or docs that operators rely on), update
**both** changelog files in the same change set:

- [`CHANGELOG.md`](CHANGELOG.md) (English)
- [`CHANGELOG-ru.md`](CHANGELOG-ru.md) (Russian)

Keep the two files aligned: same section (`Added` / `Changed` / `Fixed` /
`Security` and the Russian equivalents), same factual content, and matching
technical terms where practical.

Skip changelog updates only for trivial non-user-facing edits (typos in comments,
pure refactor with no behavior change, test-only churn, or chore that does not
affect operators or the public contract).

Prefer a short, high-level bullet under `[Unreleased]` rather than listing every
touched file or commit.
