# Extension DuckDB `qvd` — `read_qvd()` / `COPY TO (FORMAT qvd)`

Extension DuckDB **100 % Rust** pour **lire et écrire** les fichiers Qlik
**QVD**. La **lecture** s'appuie sur [`qvd` (qvdrs)](https://github.com/bintocher/qvdrs)
(reader **streaming** à mémoire bornée) ; l'**écriture** sur
[OpenQVD](https://github.com/Sigilweaver/OpenQVD) (contrôle fin des tags).

```sql
SELECT * FROM read_qvd('ventes.qvd');
COPY (SELECT * FROM ventes) TO 'sortie.qvd' (FORMAT qvd);
COPY ventes FROM 'sortie.qvd' (FORMAT qvd);
```

## État du projet

**Lecture fonctionnelle, vérifiée en DuckDB live (v1.5.3).** L'extension
s'enregistre, déclare la table function `read_qvd(VARCHAR)` et lit les fichiers
QVD en **streaming** via `qvdrs` : déduction d'un type DuckDB par champ (au
`bind`), puis décodage **par chunks de 2048 lignes** (`open_qvd_stream`/
`next_chunk`), un fichier ouvert à la fois. La mémoire est **bornée** (≈ tables de
symboles + un chunk), indépendamment du nombre de lignes — plus de matérialisation
de toutes les lignes. Les colonnes `DATE`/`BIGINT`/`VARCHAR` se comportent comme de
vrais types SQL (filtres, `EXTRACT`, agrégats). La logique est isolée dans
[`src/qvd.rs`](src/qvd.rs) et couverte par des tests (`cargo test --lib`).

```sql
SELECT id, price FROM read_qvd('ventes.qvd') WHERE price > 100;
```

### Correspondance des types

Le type DuckDB est déduit des **tags Qlik** de l'en-tête (lus au `bind`, sans
décoder les données — ce qui permet la projection pushdown), avec repli sur la
balise `<Type>` :

| Tag Qlik (repli `<Type>`)    | Type DuckDB |
|------------------------------|-------------|
| `$date`                      | `DATE`      |
| `$timestamp` (sans `$date`)  | `TIMESTAMP` |
| `$time`                      | `TIME`      |
| `$interval`                  | `INTERVAL`  |
| `$text` / `$ascii`           | `VARCHAR`   |
| `$integer`                   | `BIGINT`    |
| `$numeric` (sans `$integer`) | `DOUBLE`    |

Les tags sont un signal plus fiable que `<Type>` (souvent `UNKNOWN` même pour de
vraies dates). Le numéro de série Qlik (époque 1899-12-30) est converti en
`DATE`/`TIMESTAMP` natif. Vérifié sur données réelles : série `33765` →
`1992-06-10`. Les `NULL` sont préservés.

### Projection pushdown

`supports_pushdown()` est activé : DuckDB ne demande que les colonnes utiles, et
seules celles-ci sont décodées via `Qvd::from_path_projected`. Le `bind` ne lit
que l'en-tête ; le décodage des données a lieu à l'`init`, restreint aux colonnes
projetées. `SELECT count(*)` ne décode aucune colonne.

### Glob multi-fichiers

```sql
SELECT * FROM read_qvd('data/ventes_*.qvd');
```

Le motif est déployé (trié) et les lignes de tous les fichiers sont
**concaténées**. Le schéma est celui du **premier fichier** ; dans chaque fichier
les champs sont résolus **par nom** (robuste aux écarts d'ordre des colonnes), un
champ absent ressortant en `NULL`. Un motif sans correspondance lève une erreur.

### Écriture : `COPY ... TO ... (FORMAT qvd)`

```sql
COPY (SELECT id, montant, date_vente FROM ventes) TO 'export.qvd' (FORMAT qvd);
```

Implémentée via une **copy function** de l'API C (non wrappée par duckdb-rs,
pilotée en FFI dans [`src/copy.rs`](src/copy.rs)) : `bind` (types) →
`global_init` (chemin) → `sink` (accumulation) → `finalize` (écriture OpenQVD).
Types préservés et taggés pour relecture (`BIGINT`/`DOUBLE`/`VARCHAR`/`DATE`/
`TIMESTAMP`), `NULL` et UTF-8 conservés. Vérifié en round-trip `read_qvd` →
`COPY` → `read_qvd`.

**Noms de colonnes.** L'API C de la copy function n'expose pas les noms de
colonnes : par défaut les champs sont `field0`, `field1`, … On peut les fournir
explicitement via l'option `FIELD_NAMES` (les deux syntaxes sont acceptées) :

```sql
COPY (SELECT id, montant, jour FROM ventes)
  TO 'export.qvd' (FORMAT qvd, FIELD_NAMES (id, montant, jour));
-- ou :  FIELD_NAMES ['id', 'montant', 'jour']
```

Le nombre de noms doit égaler le nombre de colonnes (sinon erreur).

Limites d'écriture :
- Types écrits : BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT/FLOAT/DOUBLE/`DECIMAL`/
  VARCHAR/DATE/TIMESTAMP/TIME/INTERVAL (le `DECIMAL` est converti en `DOUBLE`,
  comme le permet le format QVD). Les autres (ex. `HUGEINT`, types non signés)
  exigent un `CAST`.
- Les `INTERVAL` avec une composante en **mois** sont approximés à 30 jours/mois
  (le format QVD n'a pas de notion de mois).

### Import : `COPY table FROM 'fichier.qvd' (FORMAT qvd)`

```sql
CREATE TABLE ventes(nom VARCHAR, date_vente DATE, total BIGINT);
COPY ventes FROM 'ventes.qvd' (FORMAT qvd);
```

Dans l'API C, le `COPY ... FROM` délègue à une **table function** ; comme
duckdb-rs n'expose pas le `duckdb_table_function` brut d'un `VTab`, elle est
construite en FFI dans [`src/copy_from.rs`](src/copy_from.rs) et réutilise la
lecture/typage de `read_qvd`. Tous les types (dont `DATE`/`TIME`/`INTERVAL`) et
les `NULL` sont restitués ; round-trip `COPY TO` → `COPY FROM` vérifié.

(Équivalent à `INSERT INTO ventes SELECT * FROM read_qvd('ventes.qvd')`.)

### Limitations connues (améliorations futures)

- Typage piloté par les tags Qlik ; un QVD sans tags retombe sur `<Type>` puis
  `VARCHAR` par défaut (les fichiers produits par Qlik sont toujours taggés).
- **Lecture en streaming à mémoire bornée** (≈ tables de symboles + un chunk).
  En revanche `qvdrs` décode **toutes** les tables de symboles du fichier (pas de
  projection au niveau symboles) ; la projection s'applique à l'émission.
- L'**écriture** (`COPY TO`) accumule encore toutes les lignes en mémoire (writer
  OpenQVD).
- Glob local uniquement (pas de système de fichiers DuckDB : ni httpfs ni S3).

## Structure

| Fichier | Rôle |
|---|---|
| `src/lib.rs` | Entrypoint C-API + VTab `read_qvd` (plomberie DuckDB, streaming) |
| `src/qvd.rs` | Lecture streaming `qvdrs` : schéma, typage, scan par chunks, conversions + tests |
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
- [x] Types temporels natifs `DATE`/`TIMESTAMP`/`TIME`/`INTERVAL` (séries Qlik converties).
- [x] Projection pushdown via `from_path_projected` (seules les colonnes utiles décodées).
- [x] Glob `read_qvd('data/*.qvd')` (lignes concaténées, résolution par nom).
- [x] Écriture `COPY ... TO ... (FORMAT qvd)` (copy function FFI ; round-trip vérifié).
- [x] Préserver les noms de colonnes à l'écriture (option `FIELD_NAMES`).
- [x] Écriture des `DECIMAL` (toutes largeurs i16/i32/i64/i128 → DOUBLE).
- [x] Écriture `TIME`/`INTERVAL` (round-trip temporel complet vérifié).
- [x] Import `COPY table FROM 'x.qvd'` (table function FFI ; round-trip vérifié).

## Licence

Apache-2.0, comme OpenQVD.
