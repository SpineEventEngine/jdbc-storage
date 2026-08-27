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

package io.spine.server.storage.jdbc.config

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.string.shouldContain
import io.spine.base.Identifier.newUuid
import io.spine.core.BoundedContextNames.newName
import io.spine.core.Event
import io.spine.server.ContextSpec
import io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Tests that [TableSpecs] rejects distinct storages whose table names alias
 * after the replacement of the characters prohibited in table names.
 */
@DisplayName("`TableSpecs` should")
internal class TableNameCollisionSpec {

    @Test
    fun `reject two contexts whose names alias to one event table`() {
        h2Factory().use { factory ->
            factory.createEventStore(ContextSpec.singleTenant("Sales.EU"))

            val exception = shouldThrow<IllegalStateException> {
                factory.createEventStore(ContextSpec.singleTenant("Sales_EU"))
            }

            val message = exception.message.shouldNotBeNull()
            message shouldContain "Sales.EU"
            message shouldContain "Sales_EU"
            message shouldContain "Sales_EU_Event"
        }
    }

    @Test
    fun `tolerate the same storage resolving its table repeatedly`() {
        h2Factory().use { factory ->
            val context = ContextSpec.singleTenant("Billing")
            factory.createEventStore(context)
            factory.createEventStore(context)
        }
    }

    @Test
    fun `reject a custom name clashing with a derived one`() {
        val factory = JdbcStorageFactory.newBuilder()
            .setDataSource(whichIsStoredInMemory(newUuid()))
            .setTypeMapping(H2_2_4)
            .setTableName(newName("Billing"), Event::class.java, "Shipping_Event")
            .build()
        factory.use {
            it.createEventStore(ContextSpec.singleTenant("Shipping"))

            shouldThrow<IllegalStateException> {
                it.createEventStore(ContextSpec.singleTenant("Billing"))
            }
        }
    }

    @Test
    fun `compare the names truncated to the strictest identifier limit`() {
        h2Factory().use { factory ->
            val prefix = "A".repeat(63)
            factory.createEventStore(ContextSpec.singleTenant(prefix + "One"))

            shouldThrow<IllegalStateException> {
                factory.createEventStore(ContextSpec.singleTenant(prefix + "Two"))
            }
        }
    }
}
