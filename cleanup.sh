#!/usr/bin/env bash

set -euo pipefail
force=1


repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$repo_root"


git -c core.quotePath=false \
  ls-files --others --ignored --exclude-from=.gitignore -z |
while IFS= read -r -d '' path; do
  [[ -z "$path" ]] && continue
  if (( force )); then
    rm -rf -- "$path"
    printf 'removed: %s\n' "$path"
  else
    printf '[DRY] would remove: %s\n' "$path"
  fi
done

if (( ! force )); then
  echo "Dry run complete. Re-run with --force to delete."
fi
