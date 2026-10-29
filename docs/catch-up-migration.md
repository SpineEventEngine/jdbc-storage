# Migrating the catch-up jobs

*Applies when upgrading to the library versions based on `core-jvm`
`2.0.0-SNAPSHOT.610` or later.*

## What changed

A catch-up job records the progress of one projection catch-up. Previously, the jobs
were `CatchUp` messages in a message storage. Starting with `core-jvm`
`2.0.0-SNAPSHOT.610`, each job is the state of a process manager, and the jobs are
stored as entity records. This page applies the upgrade rule of the `core-jvm`
[migration guide][core-guide] to the SQL tables of this library. See the `core-jvm`
pull request [#1699][core-pr] for the rationale.

Both versions keep the jobs in the same table, `spine_server_catchup_CatchUp`, named
after the stored `CatchUp` type. The new storage needs three columns the old table
does not have: `deleted`, `archived`, and `version`.

The library creates a table only if it is missing, and never changes the columns of
an existing one. As a result, an upgraded application keeps the old table and starts
without errors. Then every read of the jobs fails on the missing `archived` column,
and every write fails on the missing `deleted` column. The `Delivery` reads the jobs
while it delivers messages, so message delivery fails as well, not only the catch-ups.

Deleting the rows does not help, because the columns are the problem, not the data.
**Drop the table instead.** The stored jobs cannot be carried over anyway: the old rows
hold `CatchUp` messages, while the new storage reads entity records.

No other table changes its columns in this upgrade.

## Upgrading

The old and the new versions cannot share the job table, so do not run them side by
side. Upgrade all nodes of the deployment at once.

> [!IMPORTANT]
> Reference the table names unquoted, exactly as the statements below do. The library
> emits them without quotes, so the engine folds the stored names: for example, H2 stores
> the job table as `SPINE_SERVER_CATCHUP_CATCHUP`, PostgreSQL as
> `spine_server_catchup_catchup`, and MySQL keeps it as written (subject to
> `lower_case_table_names`). An unquoted reference folds the same way and matches.
> A [custom table name](#a-custom-table-name) may need quotes.

1. **Let the running catch-ups finish.** Check that every job is completed:

   ```sql
   SELECT ID, projection_type, status
       FROM spine_server_catchup_CatchUp
       WHERE status <> 4;
   ```

   With the default type mapping, the `status` column holds the ordinal of the
   `CatchUpStatus` value, and `4` stands for `COMPLETED`. An empty result means that
   no catch-up is running. Do not start new catch-ups until the upgrade is done.

2. **Let the `Delivery` deliver every message.** Stop sending commands and events to
   the application, and wait until the inboxes hold no message left to deliver:

   ```sql
   SELECT COUNT(*)
       FROM spine_server_delivery_InboxMessage
       WHERE status <> 3;
   ```

   `3` stands for `DELIVERED`: the delivered messages may stay in the table for
   the deduplication window, and the query must return `0`. The new version cannot
   deliver a shard maintenance message that the old version wrote, and dropping the job
   table does not remove such messages. Each of them fails the delivery run.

3. **Stop every node** that runs the old version.

4. **Drop the job table:**

   ```sql
   DROP TABLE spine_server_catchup_CatchUp;
   ```

5. **Start the new version.** It creates the table again, with the new columns.

There is no shortcut for a catch-up that is still running: it has shard maintenance
messages in flight, so let it complete before the upgrade.

## A custom table name

If the job table is named via `setTableName(CatchUp.class, ...)`, as described in
[Customization](tables.md#customization), drop that table instead. The custom name
still applies after the upgrade: both versions look it up by the `CatchUp` type.
A name that contains characters other than Latin letters, digits, and `_` or that
matches a reserved SQL word is created quoted, so reference it quoted as well.

Likewise, if the inbox table is named via `setTableName(InboxMessage.class, ...)`,
query that table in step 2.

[core-guide]: https://github.com/SpineEventEngine/core-jvm/blob/master/docs/migration/catch-up-in-system-contexts.md
[core-pr]: https://github.com/SpineEventEngine/core-jvm/pull/1699
