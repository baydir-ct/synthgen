#!/usr/bin/env bash
# Build dist/synthgen_customer_kit.zip: the wheel, the profiling notebook and the instructions.
set -euo pipefail
cd "$(dirname "$0")/.."

rm -rf dist
uv build --wheel -q
kit=dist/synthgen_customer_kit
mkdir -p "$kit"
cp dist/synthgen-*.whl "$kit/"
cp notebooks/01_profile_pass1.py "$kit/"
cp docs/customer_runbook.md "$kit/INSTRUCTIONS.md"
(cd dist && zip -qr synthgen_customer_kit.zip synthgen_customer_kit)
echo "dist/synthgen_customer_kit.zip"
unzip -l dist/synthgen_customer_kit.zip
