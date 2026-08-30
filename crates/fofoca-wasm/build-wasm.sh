#!/usr/bin/env bash
# Build fofoca-wasm for the browser: cargo to a .wasm, wasm-bindgen to the ES
# glue, landing in packages/fofoca-wasm/wasm/. Adapted from the light-cycles
# example's build-wasm.sh — same toolchain gotchas, no wasm-pack and no
# bundler plugin on purpose.
set -euo pipefail

cd "$(dirname "$0")"

TARGET=wasm32-unknown-unknown
OUT_DIR="../../packages/fofoca-wasm/wasm"

if ! rustup target list --installed | grep -q "$TARGET"; then
  echo "error: the $TARGET target is not installed" >&2
  echo "  rustup target add $TARGET" >&2
  exit 1
fi

if ! command -v wasm-bindgen >/dev/null; then
  echo "error: wasm-bindgen is not installed" >&2
  echo "  cargo install wasm-bindgen-cli" >&2
  exit 1
fi

# ring's C core needs a clang with a wasm backend; Apple clang has none.
if [[ "$(uname)" == "Darwin" ]]; then
  if ! command -v brew >/dev/null || [[ ! -x "$(brew --prefix llvm 2>/dev/null)/bin/clang" ]]; then
    echo "error: Homebrew LLVM is required on macOS (brew install llvm)" >&2
    exit 1
  fi
  LLVM_CLANG="$(brew --prefix llvm)/bin/clang"
  export CC="$LLVM_CLANG"
  export CC_wasm32_unknown_unknown="$LLVM_CLANG"
fi

cargo build --release --target "$TARGET" -p fofoca-wasm

mkdir -p "$OUT_DIR"
wasm-bindgen \
  --target web \
  --out-dir "$OUT_DIR" \
  "$(dirname "$(cargo locate-project --workspace --message-format plain)")/target/$TARGET/release/fofoca_wasm.wasm"

echo "built $OUT_DIR/fofoca_wasm.js"
