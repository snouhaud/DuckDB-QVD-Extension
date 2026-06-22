#!/usr/bin/env bash
# Construit qvd.duckdb_extension sans le pipeline make/venv : compile la dylib
# puis appose le footer de métadonnées DuckDB avec le script de extension-ci-tools.
#
# Particularité : pour l'ABI C_STRUCT_UNSTABLE, DuckDB exige que la version
# stampée corresponde EXACTEMENT à celle de l'hôte qui charge l'extension. On
# détecte donc la version du `duckdb` local. (Le crate est compilé contre
# l'API C v1.5.2 ; les patches v1.5.x partagent la même struct, donc charger
# dans v1.5.3 fonctionne.)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLCHAIN="${RUST_TOOLCHAIN:-1.95.0}"
EXT_NAME="qvd"
EXT_VERSION="1.0.1"

# 1. Compiler la bibliothèque native.
( cd "$ROOT" && cargo "+$TOOLCHAIN" build )

# 2. Plateforme DuckDB.
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  PLATFORM=osx_arm64 ;;
  Darwin-x86_64) PLATFORM=osx_amd64 ;;
  Linux-x86_64)  PLATFORM=linux_amd64 ;;
  Linux-aarch64) PLATFORM=linux_arm64 ;;
  *) echo "Plateforme non gérée : $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

# 3. Version de l'hôte DuckDB (doit matcher pour l'ABI instable).
DUCKDB_VERSION="$(duckdb --version | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
echo "Plateforme=$PLATFORM  DuckDB=$DUCKDB_VERSION"

# 4. Apposer le footer de métadonnées.
mkdir -p "$ROOT/build"
python3 "$ROOT/extension-ci-tools/scripts/append_extension_metadata.py" \
  -l "$ROOT/target/debug/lib${EXT_NAME}.dylib" \
  -o "$ROOT/build/${EXT_NAME}.duckdb_extension" \
  -n "$EXT_NAME" \
  -dv "$DUCKDB_VERSION" \
  -ev "$EXT_VERSION" \
  -p "$PLATFORM" \
  --abi-type C_STRUCT_UNSTABLE

echo "OK → $ROOT/build/${EXT_NAME}.duckdb_extension"
echo "Test : duckdb -unsigned -c \"LOAD '$ROOT/build/${EXT_NAME}.duckdb_extension'; SELECT * FROM read_qvd('fichier.qvd');\""
