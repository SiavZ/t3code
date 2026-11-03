#!/usr/bin/env bash
# Local CI for this fork: runs .github/workflows/ci.yml's jobs for one commit in
# a Linux container on this machine, records the result, and reports it to
# GitHub as the `local-ci` commit status that main's branch protection requires.
#
#   scripts/local-ci/run.sh                 the HEAD commit
#   scripts/local-ci/run.sh <rev>           any committed revision
#   scripts/local-ci/run.sh --check <rev>   exit 0 only if <rev> already passed
#   LOCAL_CI_JOBS="lint build" run.sh       a subset (never recorded as a pass)
#   LOCAL_CI_NO_STATUS=1 run.sh             skip the GitHub status
#
# Only committed content is tested: the container clones the commit, so
# uncommitted edits and the host's git config, editors, and caches never leak in.
set -uo pipefail

root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
# Worktrees share one object store, which the container clones from.
git_dir=$(cd "$root" && cd "$(git rev-parse --git-common-dir)" && pwd)
state=${LOCAL_CI_HOME:-$HOME/.local/state/t3code-local-ci}
image=t3code-local-ci
context=$(cd "$(dirname "$0")" && pwd)
# A dedicated Colima VM keeps CI off the default Docker VM and its disk.
profile=${LOCAL_CI_COLIMA_PROFILE:-t3ci}
previous_context=$(docker context show 2>/dev/null)
export DOCKER_HOST=${LOCAL_CI_DOCKER_HOST:-unix://$HOME/.colima/$profile/docker.sock}
mkdir -p "$state/passed" "$state/logs"

if [[ "${1:-}" == "--check" ]]; then
  sha=$(git -C "$root" rev-parse --verify "${2:-HEAD}^{commit}") || exit 2
  [[ -f "$state/passed/$sha" ]]
  exit
fi

sha=$(git -C "$root" rev-parse --verify "${1:-HEAD}^{commit}") || exit 2
log="$state/logs/$sha.log"
repo=$(git -C "$root" remote get-url origin | sed -E 's#^(https://github.com/|git@github.com:)##; s#\.git$##')
full=$([[ -z "${LOCAL_CI_JOBS:-}" ]] && echo 1 || echo 0)

status() {
  [[ -n "${LOCAL_CI_NO_STATUS:-}" || $full -eq 0 ]] && return 0
  gh api --silent -X POST "repos/$repo/statuses/$sha" \
    -f state="$1" -f context=local-ci -f description="$2" ||
    echo "local-ci: could not report '$1' to GitHub; the local result still counts." >&2
}

# One run per machine: two runs at once would starve each other's server tests.
exec 9>"$state/lock"
if ! lockf -t 0 9 2>/dev/null; then
  echo "local-ci: another run holds $state/lock; waiting for it." >&2
  lockf 9 || exit 2
fi

if ! docker info >/dev/null 2>&1; then
  [[ -z "${LOCAL_CI_DOCKER_HOST:-}" ]] && command -v colima >/dev/null || {
    echo "local-ci: Docker at $DOCKER_HOST is not reachable." >&2
    exit 2
  }
  echo "local-ci: starting the '$profile' Colima VM." >&2
  colima start --profile "$profile" --cpus 8 --memory 16 --disk 80 >/dev/null 2>&1 || exit 2
  # colima start makes the new VM the default Docker context; keep the user's.
  [[ -n "$previous_context" ]] && docker context use "$previous_context" >/dev/null 2>&1
fi
docker build --quiet --tag "$image" "$context" >/dev/null || exit 2

echo "local-ci: testing ${sha:0:12} (log: $log)"
status pending "Running on $(hostname -s)"
started=$SECONDS
docker run --rm \
  --env LOCAL_CI_SHA="$sha" \
  --env LOCAL_CI_JOBS="${LOCAL_CI_JOBS:-}" \
  --env LOCAL_CI_FILES="${LOCAL_CI_FILES:-}" \
  --volume "$git_dir:/src:ro" \
  --volume t3code-local-ci-pnpm-store:/pnpm-store \
  --volume t3code-local-ci-cargo:/usr/local/cargo/registry \
  --volume "$context/jobs.sh:/jobs.sh:ro" \
  "$image" bash /jobs.sh 2>&1 | tee "$log"
code=${PIPESTATUS[0]}
minutes=$(((SECONDS - started) / 60))

if [[ $code -eq 0 ]]; then
  [[ $full -eq 1 ]] && touch "$state/passed/$sha"
  status success "Passed in ${minutes}m on $(hostname -s)"
  echo "local-ci: ${sha:0:12} passed."
else
  status failure "Failed after ${minutes}m on $(hostname -s); see $log"
  echo "local-ci: ${sha:0:12} FAILED. Log: $log" >&2
fi
exit "$code"
