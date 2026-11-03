# Fork CI and code management (SiavZ/t3code)

This fork does not use GitHub Actions. CI runs on the maintainer's Mac, and GitHub keeps branches,
pull requests, merges, and history. Upstream (`pingdotgg/t3code`) is unaffected: its workflows
stay in `.github/workflows` so upstream merges apply cleanly, but Actions are disabled on the fork.

## How a change reaches main

1. Work on a branch and push it to `origin` as usual. Branch pushes are not gated.
2. Run local CI on the commit you want on main:

   ```sh
   vp run ci:local            # HEAD
   vp run ci:local <rev>      # any committed revision
   ```

   It runs `.github/workflows/ci.yml`'s jobs (lint, build and typecheck, package, web and server
   tests, Rust, release smoke) in an Ubuntu 24.04 container on a clean clone of that commit, and
   reports the result to GitHub as the `local-ci` commit status.

3. Merge on GitHub (the PR shows `local-ci`), or push main directly. Branch protection on `main`
   requires a successful `local-ci` status on the exact commit, and the `pre-push` hook refuses to
   push an untested commit to `origin/main`.

A merge commit is a new commit: run `vp run ci:local` on it before pushing main. A GitHub merge
button produces its commit on GitHub, so prefer merging locally and pushing:

```sh
git switch main && git merge --no-ff <branch>
vp run ci:local && git push origin main
```

## Requirements

- Colima. Local CI runs in its own Colima VM, profile `t3ci` (8 CPUs, 16 GiB, 80 GiB disk), and
  starts it when needed, so CI never fills or slows the default Docker VM. The default Docker
  context stays as it was. Set `LOCAL_CI_DOCKER_HOST` to use another Docker daemon.
- `gh` signed in with `repo` scope, to post the status. Without it the run still records locally.

## Details

- Only committed content is tested. The container clones from this checkout's `.git`, read-only,
  so uncommitted edits, the host's git config, installed editors, and caches cannot change results.
  Several upstream tests are sensitive to those on a macOS host, which is why CI runs in Linux.
- Results and logs live in `~/.local/state/t3code-local-ci` (`passed/<sha>`, `logs/<sha>.log`).
  `vp run ci:local --check <rev>` exits 0 only for a recorded pass.
- `LOCAL_CI_JOBS="lint build" vp run ci:local` runs a subset for iteration. Subsets never count as a
  pass and post no status.
- `LOCAL_CI_SKIP=1 git push origin main` bypasses the hook once. Branch protection still applies
  unless you push as an admin, which GitHub allows.
- The pnpm store and Cargo registry persist in Docker volumes named `t3code-local-ci-*`; delete
  them to start cold. Chromium is baked into the image: when a dependency bump changes the
  Playwright version, update `PLAYWRIGHT_VERSION` in `scripts/local-ci/Dockerfile`.

## Not covered locally

- Mobile native static analysis (`vp run lint:mobile`, macOS-only in upstream CI) and the Windows
  test workflow. Run `vp run lint:mobile` on the host when touching Swift or Kotlin.
- Release, desktop signing, EAS, relay deploys, and previews. They need upstream's secrets and
  infrastructure. The fork deploys by installing a build on its own hosts.
