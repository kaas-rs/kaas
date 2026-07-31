# Releasing kaas

Releases are tag-driven. Pushing a semver tag to `main` triggers the
`release` workflow (`.github/workflows/docker-publish.yml`), which builds and
publishes the broker image, the operator image, and the Helm chart to GHCR.

Every `v*` tag builds the broker and operator from `bins/*/Dockerfile`
and publishes to `kaas[-preview]` / `kaas-operator[-preview]`. The
`v0.2.x` line is the first cut from the Rust workspace; `v0.1.x` tags
predate it (the version line jumped from `v0.1.190-preview` to
`v0.2.0-preview` at the cutover — the one pre-authorised exception to
patch-bump-only).

## Versioning

- Semver: `vMAJOR.MINOR.PATCH[-PRERELEASE]`.
- While the project is pre-1.0, every release uses the `-preview` suffix
  (e.g. `v0.1.3-preview`). Pre-release tags publish to separate image and
  chart names so they can't be mistaken for stable artifacts.
- **Bump the patch for every release. Never re-cut an existing tag.** Tags
  are immutable; downstream consumers (Helm, ArgoCD, image pulls) cache by
  digest, and re-pointing a tag silently breaks them.

History so far:

| Tag                | Headline change                                                  |
| ------------------ | ---------------------------------------------------------------- |
| `v0.1.0-preview`   | First preview cut — broker registers discovered topics on start. |
| `v0.1.1-preview`   | Helm chart honours `auth.enabled=false` via env.                 |
| `v0.1.2-preview`   | Broker shares one `FlockLock` between engine and produce path.   |
| `v0.1.3-preview`   | Coordinator caps initial rebalance delay for new groups.         |

## Compatibility policy (pre-v1)

**kaas makes no backwards-compatibility promises before a v1 release.**
Preview releases (`v0.x`) may freely break:

- the on-disk layout (segment/manifest/snapshot formats, directory
  structure, `__cluster/` state files),
- CRD schemas and their semantics,
- env-var and chart-values contracts,
- rolling-upgrade behavior between adjacent previews.

Backwards compatibility at this stage adds complexity that is not worth
carrying. Migration shims, legacy-layout adoption paths, and
mixed-version guards are **optional** — add one only when it is
near-free, and feel free to drop existing ones when they get in the way.

**One promise is kept: a release that leaves the CRD schemas unchanged
supports an in-place rolling update from the immediately preceding
preview release.** The transient mixed-version window of a normal
rolling restart (heartbeats, assignment/state file formats, wire
surface between adjacent versions) must keep working in that case — a
routine version bump must not require touching the deployment. CRD
schema changes are the breaking-release signal: a release that changes
them may break anything, and its supported upgrade path is
**delete the deployment and start fresh.** Call the break out in the
release notes / tag message so the operator knows a fresh deploy is
required; that is the entire obligation. (In practice purely *additive*
CRD changes have rolled fine too, but the promise only covers
CRD-unchanged releases.)

This policy flips at v1: from then on, on-disk formats and CR schemas
version properly and upgrades are supported.

## What gets published

The workflow strips the leading `v` and uses the remainder as both the image
tag and the Helm chart version (so `v0.1.3-preview` → `0.1.3-preview`).

Pre-release tags (`v*-*`) publish to:

- `ghcr.io/kaas-rs/kaas-preview:<version>`
- `ghcr.io/kaas-rs/kaas-operator-preview:<version>`
- `oci://ghcr.io/kaas-rs/charts/kaas:<version>`

Stable tags (no pre-release suffix) publish to the un-suffixed names
(`kaas`, `kaas-operator`). No stable tag has been cut yet.

Images are built `linux/amd64` only.

## Cutting a release

1. Make sure `main` is green and contains the changes you want to ship.
2. Pick the next patch version. Look at the latest tag:
   ```bash
   git tag -l 'v*' | sort -V | tail -n1
   ```
   Bump the patch (`v0.1.3-preview` → `v0.1.4-preview`).
3. Tag the tip of `main` and push the tag:
   ```bash
   git tag v0.1.4-preview
   git push origin v0.1.4-preview
   ```
   The release workflow runs automatically on the tag push; no manual
   approval gate.
4. After the run completes, verify the artifacts exist:
   ```bash
   # chart
   helm pull oci://ghcr.io/kaas-rs/charts/kaas --version 0.1.4-preview
   # images (any registry-aware tool works)
   docker buildx imagetools inspect \
     ghcr.io/kaas-rs/kaas-preview:0.1.4-preview
   ```

The `Chart.yaml` `version`/`appVersion` fields are overridden by the CI
packaging step (`helm package --version --app-version`); you do **not** need
to bump them in-tree before tagging.

## Hotfixes

Same flow — there is no separate hotfix branch. Land the fix on `main`,
bump the patch, tag, push. If a bad release ships, cut a new patch with the
fix; do not delete or move the broken tag.

## Infrastructure notes

- The release job runs on the in-cluster `arc-runner-set` (ARC runners
  defined in the `k3s-cluster` repo under `apps/arc-runners`). It needs the
  DinD sidecar for `docker buildx`.
- Image and chart pushes use the workflow's `GITHUB_TOKEN` with
  `packages: write`; no PATs required.
- Cosign signing was removed (commits `54bcf9e`, `60e1810`); released
  artifacts are not signed today.
