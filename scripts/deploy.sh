#!/usr/bin/env bash
# Build the site and deploy it to Cloudflare Pages (comed.purr.io).
#
# --branch=main is load-bearing: Pages treats any other branch as a preview
# deployment, which gets its own *.pages.dev URL and leaves comed.purr.io
# pointing at whatever was last pushed to main.
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT="power-puddle"

npm --prefix site ci
npm --prefix site run build

npx wrangler pages deploy site/dist \
  --project-name "$PROJECT" \
  --branch=main

echo "Deployed. https://comed.purr.io"
