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

package io.spine.server.storage.jdbc

import io.kotest.matchers.shouldBe
import io.spine.base.Identifier.newUuid
import io.spine.core.BoundedContextNames.newName
import io.spine.core.Event
import io.spine.core.EventId
import io.spine.server.storage.RecordSpec
import io.spine.server.storage.StorageGroup
import io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory
import io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Tests the custom naming of the event table of a Bounded Context via
 * [JdbcStorageFactory.Builder.setTableName].
 */
@DisplayName("`JdbcStorageFactory` should")
internal class ContextTableNameSpec {

    @Test
    fun `honor the custom name of a context-grouped event table`() {
        val factory = newFactory {
            it.setTableName(newName("Billing"), Event::class.java, "billing_events")
        }
        factory.use {
            val spec = it.tableSpecFor(eventSpec(), StorageGroup.of(newName("Billing")))
            spec.tableName() shouldBe "billing_events"
        }
    }

    @Test
    fun `not apply a single-type custom name to the event table of a context`() {
        val factory = newFactory {
            it.setTableName(Event::class.java, "all_events")
        }
        factory.use {
            val spec = it.tableSpecFor(eventSpec(), StorageGroup.of(newName("Billing")))
            spec.tableName() shouldBe "Billing_Event"
        }
    }

    private fun newFactory(
        configure: (JdbcStorageFactory.Builder) -> Unit
    ): JdbcStorageFactory {
        val builder = JdbcStorageFactory.newBuilder()
            .setDataSource(whichIsStoredInMemory(newUuid()))
            .setTypeMapping(H2_2_4)
        configure(builder)
        return builder.build()
    }

    /**
     * Composes a record specification equal in identity to the one used by
     * the event store: `Event` records under `EventId` identifiers.
     */
    private fun eventSpec(): RecordSpec<EventId, Event> =
        RecordSpec(EventId::class.java, Event::class.java) { event ->
            requireNotNull(event.id)
        }
}
