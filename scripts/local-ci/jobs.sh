#!/usr/bin/env bash
# Runs inside the local CI container: the jobs of .github/workflows/ci.yml on a
# clean clone of one commit. Ends with a pass/FAIL summary per job and exits
# nonzero when any job failed. LOCAL_CI_JOBS selects a subset.
set -uo pipefail

: "${LOCAL_CI_SHA:?}"
work=/work
results=()
failed=0

job() {
  local name=$1
  shift
  echo
  echo "::group::${name}"
  local started=$SECONDS
  if "$@"; then
    results+=("pass  ${name}  $((SECONDS - started))s")
  else
    results+=("FAIL  ${name}  $((SECONDS - started))s")
    failed=1
  fi
  echo "::endgroup::"
}

setup() {
  git config --global --add safe.directory '*'
  git config --global init.defaultBranch main
  find "$work" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  git clone --quiet --no-checkout /src "$work"
  cd "$work"
  git -c advice.detachedHead=false checkout --quiet "$LOCAL_CI_SHA"
  npm install --global --prefix /pnpm --silent \
    "pnpm@$(node -p 'require("./package.json").packageManager.split("@")[1]')"
  pnpm install --frozen-lockfile --store-dir /pnpm-store
}

lint() {
  local assets
  assets=$(git ls-files .github/pr-assets)
  if [[ -n "$assets" ]]; then
    printf 'PR evidence must be uploaded to GitHub, not committed:\n%s\n' "$assets" >&2
    return 1
  fi
  vp run --filter @t3tools/desktop ensure:electron &&
    vp run knip:check &&
    vp run --filter @t3tools/source-control-github generate --check &&
    vp run --filter t3 generate --check &&
    vp check
}

build() {
  vpr typecheck &&
    vp run build:desktop &&
    node apps/desktop/scripts/verify-preload-bundle.mjs
}

test_packages() {
  python3 -B .github/scripts/stage-preview-bundle.test.py &&
    node --test .github/scripts/check-nightly-release.test.cjs \
      .github/scripts/vercel-deployment-output.test.cjs &&
    vp run --parallel --concurrency-limit 4 \
      --filter '!t3' --filter '!@t3tools/monorepo' --filter '!@t3tools/web' test
}

test_web() {
  vp run --filter @t3tools/web test &&
    vp run --filter @t3tools/web build &&
    node apps/web/scripts/verify-built-boot.ts
}

# apps/server runs one test file at a time, so CI shards it over runners. One
# local container runs the whole suite in sequence instead. Chromium for both
# browser suites is baked into the image.
test_server() {
  vp run --filter t3 test
}

rust() {
  local crate
  for crate in resource-monitor kde-snap-shot hyprland-snap-shot; do
    cargo fmt --manifest-path "native/$crate/Cargo.toml" -- --check || return 1
    cargo test --locked --manifest-path "native/$crate/Cargo.toml" || return 1
  done
}

release_smoke() {
  node scripts/release-smoke.ts
}

# For reruns of chosen test files: LOCAL_CI_JOBS=files LOCAL_CI_FILES="apps/web/src/a.test.ts".
files() {
  # shellcheck disable=SC2086
  vp test run ${LOCAL_CI_FILES:?}
}

export CI=1
job setup setup
if [[ $failed -eq 0 ]]; then
  for name in ${LOCAL_CI_JOBS:-lint build test_packages test_web test_server rust release_smoke}; do
    job "$name" "$name"
  done
fi

echo
echo "local-ci summary for ${LOCAL_CI_SHA}"
for line in "${results[@]}"; do echo "  ${line}"; done
exit "$failed"
