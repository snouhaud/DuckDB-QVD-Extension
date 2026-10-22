#!/usr/bin/env bash
# Construit qvd.duckdb_extension sans le pipeline make/venv : compile la dylib
# puis appose le footer de métadonnées DuckDB avec le script de extension-ci-tools.
#
# ABI stable (C_STRUCT) : la version stampée est la version minimale de l'API C
# V1 demandée au chargement (v1.5.6, comme TARGET_DUCKDB_VERSION du Makefile) ;
# le binaire se charge sur tout DuckDB qui la fournit (1.5.6, 2.0.x, …).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLCHAIN="${RUST_TOOLCHAIN:-1.97.1}"
EXT_NAME="qvd"
EXT_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT/Cargo.toml" | head -1)"

# 1. Compiler la bibliothèque native.
( cd "$ROOT" && DUCKDB_EXTENSION_MIN_DUCKDB_VERSION=v1.5.6 cargo "+$TOOLCHAIN" build )

# 2. Plateforme DuckDB.
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  PLATFORM=osx_arm64;   LIB_EXT=dylib ;;
  Darwin-x86_64) PLATFORM=osx_amd64;   LIB_EXT=dylib ;;
  Linux-x86_64)  PLATFORM=linux_amd64; LIB_EXT=so ;;
  Linux-aarch64) PLATFORM=linux_arm64; LIB_EXT=so ;;
  *) echo "Plateforme non gérée : $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

# 3. Version minimale de l'API C V1.
DUCKDB_VERSION="v1.5.6"
echo "Plateforme=$PLATFORM  DuckDB=$DUCKDB_VERSION"

# 4. Apposer le footer de métadonnées.
mkdir -p "$ROOT/build"
python3 "$ROOT/extension-ci-tools/scripts/append_extension_metadata.py" \
  -l "$ROOT/target/debug/lib${EXT_NAME}.${LIB_EXT}" \
  -o "$ROOT/build/${EXT_NAME}.duckdb_extension" \
  -n "$EXT_NAME" \
  -dv "$DUCKDB_VERSION" \
  -ev "$EXT_VERSION" \
  -p "$PLATFORM" \
  --abi-type C_STRUCT

echo "OK → $ROOT/build/${EXT_NAME}.duckdb_extension"
echo "Test : duckdb -unsigned -c \"LOAD '$ROOT/build/${EXT_NAME}.duckdb_extension'; SELECT * FROM read_qvd('fichier.qvd');\""
