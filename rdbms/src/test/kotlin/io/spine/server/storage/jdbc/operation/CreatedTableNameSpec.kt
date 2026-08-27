/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.spine.server.storage.jdbc.operation

import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldNotContain
import io.spine.server.ContextSpec
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Pins the physical form of the created table names.
 *
 * An ordinary name — Latin letters, digits, and `_`, not a reserved word —
 * is emitted unquoted, so the engine folds it: H2 stores it in upper case,
 * PostgreSQL would store it in lower case. The migration guidance in
 * `docs/event-log-migration.md` relies on this contract: unquoted references
 * in user-run SQL fold the same way and match the stored names.
 */
@DisplayName("A table created for an ordinary name should")
internal class CreatedTableNameSpec {

    @Test
    fun `be stored under the engine-folded form of the name`() {
        h2Factory().use { factory ->
            factory.createEventStore(ContextSpec.singleTenant("Billing"))

            val names = tableNames(factory)

            names shouldContain "BILLING_EVENT"
            names shouldNotContain "Billing_Event"
        }
    }

    /**
     * Reads the names of all user tables via the JDBC metadata.
     */
    private fun tableNames(factory: JdbcStorageFactory): List<String> =
        factory.dataSource().getConnection(true).use { wrapper ->
            val tables = wrapper.get()
                .metaData
                .getTables(null, null, "%", arrayOf("TABLE", "BASE TABLE"))
            buildList {
                while (tables.next()) {
                    add(tables.getString("TABLE_NAME"))
                }
            }
        }
}
