# docs/

The kaas documentation book and its supporting files. The book is published
at **<https://kaas.rs/book/>**, rebuilt from `main` on every push. The site
root at <https://kaas.rs/> is a separate landing page living in
[kaas-rs/kaas-landing-page](https://github.com/kaas-rs/kaas-landing-page) —
see [Publishing](#publishing).

## Layout

| Path | What it is |
|---|---|
| `src/` | the book's chapters — Parts I–IV, plus `SUMMARY.md` (the table of contents) |
| `book.toml` | mdbook config: `rust` default theme, search, fold nav, mermaid + linkcheck backends |
| `mermaid.min.js`, `mermaid-init.js` | committed by `mdbook-mermaid install`; required by the build |
| `perf-results/` | recorded benchmark reports, cited by Part IV's performance chapter |
| `ARCHITECTURE.md` | pointer stub — the architecture content lives in Part I |
| `RELEASING.md` | canonical release procedure (Part IV links out to it) |
| `book/` | build output, gitignored |

## Building

```bash
cargo xtask docs           # mdbook build (html + linkcheck)
cargo xtask docs --serve   # live-reloading local preview
```

Needs `mdbook`, `mdbook-mermaid`, and `mdbook-linkcheck` on `PATH`. CI pins
them in the `docs` job of `.github/workflows/ci.yml`: **mdbook 0.4.52,
mdbook-mermaid 0.16.2, mdbook-linkcheck 0.7.7**. Keep to the 0.4.x line —
mdbook-mermaid ≥ 0.17 targets mdbook 0.5's preprocessor protocol and fails
against 0.4. Bump all three together.

## The drift gates

`cargo xtask check-docs-drift` runs in the CI `rust` job and is what stops
the compatibility claims from rotting. Three checks:

1. **Generated API matrix** — `cargo xtask gen-api-matrix` renders
   `src/compat/api-matrix.md` from the `ApiSpec` registry in
   `crates/kaas-codec/src/api/registry.rs` (the same table that builds the
   ApiVersions response), then `git diff --exit-code`. *Fix a failure by
   running `cargo xtask gen-api-matrix` and committing the result.* Adding
   or removing an API key also needs its row in `API_DOCS`
   (`xtask/src/api_matrix.rs`) — the join is exhaustiveness-checked both
   ways.
2. **API anchors** — every registered key must have exactly one
   `## <ApiName>` heading on the domain page the matrix links to.
   mdbook-linkcheck 0.7.7 does *not* validate fragments, so without this a
   renamed heading would silently break every deep link into it.
3. **Source-path scan** — every `crates/…` / `bins/…` / `scripts/…` path
   cited anywhere in `src/` must exist in the tree, so a refactor that moves
   a file fails CI instead of leaving a stale citation.

## Publishing

**This repo owns the book; it does not own the site.** `kaas.rs` is served
from GitHub Pages on
[kaas-rs/kaas-landing-page](https://github.com/kaas-rs/kaas-landing-page),
which holds the landing page at the root and mounts this book under
`book/`. Pages and the custom domain are configured there, so nothing here
deploys and no `CNAME` file is needed.

`.github/workflows/docs-publish.yml` builds the book and hands the render
over: it force-pushes `docs/book/html` to this repo's **`book-dist`**
branch (an orphan, one commit deep — a build artifact, not history), then
fires a `book-updated` repository dispatch so the landing repo redeploys.
That dispatch needs a PAT in the `LANDING_DISPATCH_TOKEN` secret (the
default `GITHUB_TOKEN` cannot reach another repo); without it the step is
skipped with a warning and the landing repo's daily cron picks the book up
instead — up to a day stale, never broken.

Editing `docs/src/` is therefore all you need to do: the publish is
automatic. What *isn't* automatic is the shape of the render. The landing
page's cards link to `book/` and `book/getting-started.html`, and the CI
`docs` job asserts both survive the build, so renaming a top-level entry
point fails here rather than in the other repo's deploy. The landing repo
re-checks every one of its `book/…` links against the pulled render before
it publishes.

## Writing conventions

- **Write for a reader who knows Kafka but not kaas.** Open pages by
  locating kaas in the Kafka mental model ("in Apache Kafka, X…; kaas
  instead…"), use Kafka ≤ 4.3 vocabulary for concepts Kafka already
  names, and tie subsystems back to the book's through-line: the three
  substitutions (quorum → Lease/CRs, replication → single writer on RWX,
  internal topics → JSON files). `src/architecture/volume-pool.md` is
  the exemplar.
- **No `gh #NN` or `crates/…` paths in narrative prose.** Contributor
  material goes in a closing `## Implementation notes (for contributors)`
  section (Part I/IV) or the `**Source**:` / `**Verified by**:` trailers
  (Part II). Open follow-ups may keep a trailing "(tracked as gh #NN)"
  parenthetical. Part III (code tour) is contributor-facing and exempt.
- **Code is the source of truth.** Where a doc and the source disagree, the
  source wins and the doc gets fixed — including when that means
  documenting a gap. Part II's partial-KIP pages lead with what's *missing*;
  don't soften them.
- **Cite real paths.** The scan enforces existence; keep citations specific
  enough to be useful (module, not just crate).
- **Cross-link instead of duplicating.** Deep architecture lives in Part I;
  per-API behaviour in Part II; crate chapters stay short and point at both.

The book was built out in six phases during 2026-07-19/20; those plan
documents were retired once complete and live in git history (see the
`docs(book): phase N` commits).
