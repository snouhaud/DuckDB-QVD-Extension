# Extension DuckDB en Rust basée sur la C Extension API.
EXTENSION_NAME=qvd
USE_UNSTABLE_C_API=1
TARGET_DUCKDB_VERSION=v1.5.2

all: configure debug

# Récupère l'outillage CI partagé (venv DuckDB + test runner SQLLogicTest).
# À lancer une fois avant `make configure`.
.PHONY: bootstrap
bootstrap:
	git clone --depth 1 https://github.com/duckdb/extension-ci-tools.git

# Règles de build/test/configure du template C-API (si l'outillage est présent).
# Le `-include` n'échoue pas tant que `make bootstrap` n'a pas été lancé.
-include extension-ci-tools/makefiles/duckdb_extension_c_api.Makefile

# --- Itération locale rapide (sans l'outillage CI) ------------------------
# Compile la bibliothèque native. Le footer DuckDB n'est PAS ajouté ici :
# pour produire un vrai .duckdb_extension chargeable, utiliser `make debug`
# après `make bootstrap && make configure`.
.PHONY: cargo-build cargo-build-release cargo-clean
cargo-build:
	cargo build

cargo-build-release:
	cargo build --release

cargo-clean:
	cargo clean
