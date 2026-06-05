# Extension DuckDB `qvd` — `read_qvd()`

Extension DuckDB **100 % Rust** exposant une table function pour lire les
fichiers Qlik **QVD**, en s'appuyant sur le crate
[OpenQVD](https://github.com/Sigilweaver/OpenQVD) (lecteur QVD mûr, sortie
Arrow).

```sql
SELECT * FROM read_qvd('ventes.qvd');
```

## État du projet

**Lecture fonctionnelle, vérifiée en DuckDB live (v1.5.3).** L'extension
s'enregistre, déclare la table function `read_qvd(VARCHAR)` et lit réellement les
fichiers QVD via OpenQVD : ouverture du fichier, déduction d'un type DuckDB par
champ, et streaming des lignes par paquets de 2048 vers DuckDB. Les colonnes
`DATE`/`BIGINT`/`VARCHAR` se comportent comme de vrais types SQL (filtres,
`EXTRACT`, agrégats). La logique est isolée dans [`src/qvd.rs`](src/qvd.rs) et
couverte par un test d'intégration (`cargo test --lib`).

```sql
SELECT id, price FROM read_qvd('ventes.qvd') WHERE price > 100;
```

### Correspondance des types

Le type DuckDB de chaque colonne est déduit des valeurs réellement décodées :

| Contenu de la colonne                      | Type DuckDB |
|--------------------------------------------|-------------|
| tag `$date`/`$timestamp`, série jour entier| `DATE`      |
| tag `$date`/`$timestamp`, composante horaire| `TIMESTAMP` |
| uniquement des entiers                     | `BIGINT`    |
| au moins un flottant, aucun texte          | `DOUBLE`    |
| au moins une chaîne                        | `VARCHAR`   |

Les champs temporels sont détectés par les **tags Qlik** (`$date`/`$timestamp`),
signal plus fiable que la balise `<Type>` (souvent `UNKNOWN`). Le numéro de série
Qlik (époque 1899-12-30) est converti en `DATE`/`TIMESTAMP` natif DuckDB.
Vérifié sur données réelles : série `33765` → `1992-06-10`. Les `NULL` sont
préservés.

### Limitations connues (améliorations futures)

- Le fichier est **entièrement matérialisé en mémoire** au moment du `bind`
  (OpenQVD charge le QVD complet) ; scan **mono-thread**.
- Pas encore de projection pushdown ni de glob `read_qvd('*.qvd')`.
- Écriture (`COPY ... TO ... (FORMAT qvd)`) non implémentée.
- Tags `$time`/`$interval` non encore mappés (TIME/INTERVAL) → numériques.

## Structure

| Fichier | Rôle |
|---|---|
| `src/lib.rs` | Entrypoint C-API + VTab `read_qvd` (plomberie DuckDB, streaming) |
| `src/qvd.rs` | Intégration OpenQVD : lecture, typage, conversion + test d'intégration |
| `src/wasm_lib.rs` | Réexport pour la cible Wasm (staticlib) |
| `Cargo.toml` | Dépendances (`duckdb`, + `openqvd`/`arrow` à activer) |
| `Makefile` | Build C-API (`make debug`/`release`) + cibles cargo rapides |
| `test/sql/read_qvd.test` | Test SQLLogicTest de fumée |

## Build

### Itération rapide (bibliothèque native)

```sh
make cargo-build        # ou : cargo build
```

> Le build de la feature `loadable-extension` du crate `duckdb` génère des
> bindings et peut nécessiter **libclang** (bindgen) installé sur la machine.

### Extension chargeable (`.duckdb_extension`)

Chemin recommandé (compile + appose le footer de métadonnées, sans venv) :

```sh
git clone --depth 1 https://github.com/duckdb/extension-ci-tools.git
./scripts/build-extension.sh        # → build/qvd.duckdb_extension
```

Le script détecte la plateforme et la **version du `duckdb` local**. Pour l'ABI
C_STRUCT_UNSTABLE, DuckDB exige que la version stampée corresponde exactement à
l'hôte ; le crate vise l'API C v1.5.2 et les patches v1.5.x partagent la même
struct (testé : chargé dans **v1.5.3**).

Chargement (extension non signée) :

```sh
duckdb -unsigned -c "LOAD 'build/qvd.duckdb_extension'; \
  SELECT * FROM read_qvd('QVD-Examples/Ventes.qvd');"
```

> Le pipeline officiel `make bootstrap && make configure && make debug` reste
> disponible, mais `make configure` installe un venv Python (duckdb + runner
> sqllogictest) qui peut échouer selon la version de Python.

## Tests

```sh
cargo +1.95.0 test --lib    # test d'intégration : génère un QVD puis le relit
```

Le test (dans [`src/qvd.rs`](src/qvd.rs)) couvre entiers, flottants, texte,
`NULL` et un champ « dual » typé DATE. `--lib` cible la lib (l'« example » Wasm
a une contrainte de modules distincte).

Pour tester sur de vrais QVD : déposer des fichiers dans `test/data/` et adapter
[`test/sql/read_qvd.test`](test/sql/read_qvd.test).

## Feuille de route

- [x] Lecture `read_qvd('fichier.qvd')` avec typage BIGINT/DOUBLE/VARCHAR + NULL.
- [x] Types temporels natifs `DATE`/`TIMESTAMP` (conversion du numéro de série Qlik).
- [ ] Projection pushdown (OpenQVD expose `from_path_projected`).
- [ ] Glob `read_qvd('data/*.qvd')`.
- [ ] Écriture `COPY ... TO ... (FORMAT qvd)` (dépend de l'état de l'API copy C).

## Licence

Apache-2.0, comme OpenQVD.
