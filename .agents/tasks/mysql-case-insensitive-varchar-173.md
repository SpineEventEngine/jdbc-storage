# Case-insensitive `VARCHAR` in MySQL collides with entity IDs (#173)

Issue: https://github.com/SpineEventEngine/jdbc-storage/issues/173

## Problem

MySQL non-binary string types (`VARCHAR`, `TEXT`) use a case- and
accent-insensitive collation by default. Spine stores entity identifiers and
`String` columns in `VARCHAR(512)`/`VARCHAR(255)`/`TEXT`, so two distinct
identifiers that differ only by case — e.g. a username `"name"` vs `"Name"` —
collide: writes for `"Name"` are routed to the row stored under `"name"`.

PostgreSQL and H2 compare string data case-sensitively by default, so only the
MySQL mapping is affected.

## Fix

Make the MySQL type mapping emit a binary collation for every character-based
column type. In `PredefinedMapping.MYSQL_9_7`, override:

- `STRING_255` → `VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`
- `STRING_512` → `VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`
- `STRING`     → `TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`

Implemented via a nested `MySql` helper holding the constants, mirroring the
existing `PostgreSql` helper. The collation lives in the type mapping (its
documented responsibility), so it flows into `CreateTable` for both the ID
column and data columns without touching the DDL builder.

`utf8mb4` (not the deprecated `utf8`/`utf8mb3` from the issue example) is used as
the modern, full-Unicode charset; `utf8mb4_bin` is the binary collation. The
column byte width is unchanged, so the `VARCHAR(512)` primary key stays within
InnoDB index limits.

## Verification

- `PredefinedMappingTest`: MySQL string types carry the collation; H2/Postgres
  do not.
- `CreateTableSpec`: the generated `CREATE TABLE` carries the collation on the
  ID column under the MySQL mapping.
- `MysqlIdCaseSensitivityTest` (Docker-gated): writing records with IDs `"name"`
  and `"Name"` yields two distinct rows.
- `docs/type-mapping.md`: table updated + peculiarity documented.

## Test-harness note

`JdbcStorageFactoryTestEnv.newFactory()` ran the **MySQL** mapping over an **in-memory H2**
database. That worked only because `MYSQL_9_7` and `H2_2_4` produced identical DDL. Once the
MySQL mapping started emitting `CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`, H2 could no longer
parse the generated `CREATE TABLE`. Fixed by switching the helper to `H2_2_4` (the mapping that
matches its data source); behaviour for the H2-backed tests is byte-identical to before.

## Status

Done — `:rdbms:check` green (366 tests, 0 failed; checkstyleMain, detekt, pmdMain pass). The
Docker-gated `MysqlIdCaseSensitivityTest` was confirmed to fail when the collation override is
removed, proving it guards the regression. Not committed (awaiting review/authorization).

