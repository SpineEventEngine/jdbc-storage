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

1. **Quoting and folding.** Quoting is *conditional*: `CreateTable` passes names
   through Querydsl's `SQLTemplates.quoteIdentifier(..)`, which quotes only when
   the dialect requires it (reserved word, illegal characters) — `useQuotes` is
   off on the `SQLTemplatesRegistry` path this library uses. An ordinary name is
   emitted unquoted and folded by the engine: `Billing_Event` is stored as
   `BILLING_EVENT` on H2 and `billing_event` on PostgreSQL (verified against H2's
   `INFORMATION_SCHEMA`; pinned by `CreatedTableNameSpec`). Documented SQL must
   reference ordinary names unquoted, so they fold the same way; only a
   requires-quotes name is stored verbatim and must be referenced quoted.
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
explicitly which engines were checked against items 1–5. Verify quoting claims
*empirically* (create a table, read `INFORMATION_SCHEMA`) — spotting a
`quoteIdentifier(..)` call is not enough; the call is conditional, and a bot
review on PR #182 mis-asserted verbatim storage from exactly that shortcut.
User-facing rules live in `docs/tables.md` ("Name clashes") and
`docs/type-mapping.md`.
