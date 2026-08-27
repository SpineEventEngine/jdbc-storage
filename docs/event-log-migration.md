# Migrating the event log

*Applies when upgrading to the library versions based on `core-jvm`
`2.0.0-SNAPSHOT.540` or later.*

## What changed

Previously, the event stores of all Bounded Contexts of an application wrote to one
table, `spine_core_Event`: the table name was derived from the stored record type
alone, so the events of every context — and of every System context configured to
persist its events — intermingled in that single table.

Starting with `core-jvm` `2.0.0-SNAPSHOT.540`, the event store of each context is
a grouped storage, and its records land in a per-context table: `Billing_Event`,
`Shipping_Event`, `Billing_System_Event`, and so on. See
[the event log of a Bounded Context](tables.md#the-event-log-of-a-bounded-context)
for the naming details, and the `core-jvm` pull request [#1673][core-pr] for
the rationale.

The framework creates the new tables automatically on startup. It does **not**
move the previously stored events: an upgraded application starts reading and
writing the per-context tables, while the historical events stay in
`spine_core_Event`. Deployments that rely on the stored event log — event replay,
projection catch-up, audit — should migrate the historical events before switching
the upgraded version on.

No other tables are renamed by the upgrade.

## Migrating the stored events

The rows of `spine_core_Event` carry no explicit context marker. The `type` column —
holding the qualified Proto type name of the event — is the discriminator: each
event type belongs to the domain of exactly one context.

> [!IMPORTANT]
> Quote the table names in every migration statement. The library creates its tables
> with quoted identifiers, so the mixed-case names — `Billing_Event`,
> `spine_core_Event` — are stored exactly as shown throughout this page. An unquoted
> reference is folded to `billing_event` by PostgreSQL and to `BILLING_EVENT` by H2,
> and the statement then fails with a table-not-found error. PostgreSQL and H2 quote
> with double quotes, as in the statements below; MySQL uses backticks.

For each context, insert the rows of its event types into the per-context table:

```sql
INSERT INTO "Billing_Event"
    SELECT * FROM "spine_core_Event"
    WHERE type IN (
        'acme.billing.InvoiceIssued',
        'acme.billing.PaymentReceived'
        -- ...the rest of the event types of the `Billing` context.
    );
```

The per-context tables have the same structure as the shared one, so `SELECT *`
transfers the rows as they are. The upgraded application creates the target tables
at its first start; to run the inserts before that, pre-create the tables with
`CREATE TABLE` statements mirroring the structure of `spine_core_Event`.

After the copy, verify the counts:

```sql
SELECT
    (SELECT COUNT(*) FROM "spine_core_Event") AS shared,
    (SELECT COUNT(*) FROM "Billing_Event") +
    (SELECT COUNT(*) FROM "Shipping_Event") AS split;
```

Keep `spine_core_Event` as an archive until the migrated deployment is verified;
the upgraded application does not touch it.

## Special cases

* **An event type used by several contexts.** If the same Proto event type is
  emitted by more than one context, the `type` column cannot tell their events
  apart. Decide the ownership per deployment — usually the rows belong to the
  producing context — and split by additional criteria, such as the producer
  identifiers within the serialized records.
* **A custom event table name.** A name registered via
  `setTableName(Event.class, ...)` no longer applies: the event log is a grouped
  storage now. Register the name per context instead, and use it as the target of
  the migration inserts:

  ```java
  .setTableName(BoundedContextNames.newName("Billing"), Event.class, "billing_events")
  ```

* **Contexts with clashing table names.** If two context names resolve to one
  table name once the prohibited characters are replaced, the upgraded application
  fails fast at startup; see [Name clashes](tables.md#name-clashes) for the details
  and the remediation.

[core-pr]: https://github.com/SpineEventEngine/core-jvm/pull/1673
