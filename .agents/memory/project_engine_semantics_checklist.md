---
name: engine-semantics-checklist
description: Check identifier folding, case, length, and collation semantics of every supported engine when touching names, comparisons, or emitted SQL
metadata:
  type: project
---

Whenever a change touches table or column names, name comparisons, or SQL that
users are expected to run, walk this checklist against every supported engine
(MySQL, PostgreSQL, H2) — their semantics differ in ways that pass review one
engine at a time:

1. **Quoting and folding.** This library creates tables with *quoted*
   identifiers, so mixed-case names are stored verbatim. Unquoted SQL folds:
   PostgreSQL to lowercase, H2 to uppercase. Any SQL shown in the docs must
   quote table names (double quotes on PostgreSQL/H2, backticks on MySQL).
2. **Case sensitivity of names.** MySQL may run with a case-insensitive
   `lower_case_table_names` setting: names differing only in case map onto one
   table. Never rely on letter case to distinguish tables; `TableSpecs`
   rejects case-only distinctions when claiming names.
3. **Identifier length.** The limits differ in value *and* unit: MySQL rejects
   identifiers over 64 characters with an error; PostgreSQL silently truncates
   at 63 bytes. Compare and validate names at the strictest bound (63 bytes;
   see `TableSpecs.MAX_IDENTIFIER_BYTES`).
4. **String collation.** MySQL compares non-binary string types case- and
   accent-insensitively by default; character columns must keep the
   `utf8mb4_bin` collation (see `docs/type-mapping.md`). PostgreSQL and H2
   compare case-sensitively.
5. **Type dialects.** SQL type names come from the per-engine `TypeMapping`;
   a custom MySQL mapping must re-apply the collation clause itself.

**Why:** Both external bot reviews on PR #182 found real defects in *engine
interaction* (unquoted migration SQL; case-insensitive table clash), not in
the logic itself — the same class of issue as the earlier MySQL collation
work (#173). Catching these at writing time is cheaper than in review.

**How to apply:** Before proposing name-handling code or documented SQL, state
explicitly which engines were checked against items 1–5. User-facing rules
live in `docs/tables.md` ("Name clashes") and `docs/type-mapping.md`.
