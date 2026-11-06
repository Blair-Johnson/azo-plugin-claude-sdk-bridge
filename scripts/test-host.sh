#!/usr/bin/env bash
set -euo pipefail
: "${AZO_HOST_MANIFEST:?Set AZO_HOST_MANIFEST to the integrated Agent Zoo pixi.toml (absolute path)}"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export NODE_BINARY="$PWD/.pixi/envs/default/bin/node"
export PATH="$PWD/.pixi/envs/default/bin:$PATH"
exec env -u PYTHONPATH -u PYTHONHOME pixi run --manifest-path "$AZO_HOST_MANIFEST" \
    --frozen --no-install python -m pytest -q tests/ "$@"
