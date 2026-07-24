# Contributing to sorostream-sdk

Thank you for your interest in contributing to SoroStream! This repo participates in the **Stellar Wave Program** on [Drips Wave](https://drips.network/wave).

## Wave Contributor Workflow

1. **Browse open issues** â€” find one labelled `Stellar Wave` with a complexity you're comfortable with.
2. **Apply via Drips Wave** â€” do **not** begin coding until the maintainer assigns you to the issue.
3. **Fork the repo** and create a branch:
   - Bug fixes: `fix/N-short-description`
   - Features: `feat/N-short-description`
   - Where `N` is the issue number (e.g. `feat/4-event-listener`).
4. **Write code and tests** â€” `npm test` and `npm run lint` must pass.
5. **Open a PR** â€” title must reference the issue, body must include `Closes #N`.
6. **Await review** â€” maintainer reviews and merges. Points awarded once resolved before Wave ends.

## Local Setup

```bash
npm install
npm test       # run vitest unit tests
npm run lint   # TypeScript type check
npm run build  # build with tsup
```

## Code Style

- Strict TypeScript â€” no `any` types.
- All public methods must have JSDoc comments.
- Use `bigint` for all stroop amounts.

## Changelog Format

Keep changelog entries consistent with the Keep a Changelog style. When updating the changelog, use the following structure:

- Add a section header with one of these categories: Added, Changed, Fixed, Removed, Deprecated.
- Use one bullet per entry in the format `- Short description (#issue)`.
- Keep entries short and imperative.

Example:

```md
## [Unreleased]

### Added
- Add zero cliff duration regression test (#152)
```

Before opening a PR, run `node scripts/check-changelog-format.js --staged` if you touched `CHANGELOG.md`.
