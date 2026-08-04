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

package io.spine.server.storage.jdbc.record.given

import io.spine.base.Identifier.newUuid
import io.spine.core.Event
import io.spine.core.EventId
import io.spine.server.ContextSpec
import io.spine.server.entity.AbstractEntity
import io.spine.server.entity.EntityRecord
import io.spine.server.entity.EntityStateKey
import io.spine.server.entity.entityStateKey
import io.spine.server.entity.storage.EntityEventColumns
import io.spine.server.entity.storage.EntityStateHistoryColumns
import io.spine.server.storage.RecordSpec
import io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4
import io.spine.test.storage.StgProject
import io.spine.test.storage.StgTask
import io.spine.test.storage.StgTaskId

/**
 * The test environment for the specifications covering the storages
 * grouped by a [StorageGroup][io.spine.server.storage.StorageGroup].
 */
internal object HistoryStorageTestEnv {

    /**
     * Creates a new storage factory over an in-memory H2 database.
     */
    fun h2Factory(): JdbcStorageFactory =
        JdbcStorageFactory.newBuilder()
            .setDataSource(whichIsStoredInMemory(newUuid()))
            .setTypeMapping(H2_2_4)
            .build()

    /**
     * Creates the specification of a single-tenant Bounded Context named after this class.
     */
    fun context(): ContextSpec = ContextSpec.singleTenant(HistoryStorageTestEnv::class.java.name)

    /**
     * Composes a record specification equal to the one used by
     * the [EntityEventStorage][io.spine.server.entity.storage.EntityEventStorage] journal.
     *
     * The framework composes the journal specification privately. The tests re-create it
     * to reach the table specifications of grouped storages via
     * [JdbcStorageFactory.tableSpecFor], expecting the equally-composed specifications
     * to resolve to the same tables.
     */
    fun journalSpec(): RecordSpec<EventId, Event> = RecordSpec(
        Event::class.java,
        EventId::class.java,
        Event::class.java,
        EntityEventColumns.definitions()
    ) { event -> event.id }

    /**
     * Composes a record specification equal to the one used by the
     * [EntityStateHistoryStorage][io.spine.server.entity.storage.EntityStateHistoryStorage]
     * of an entity with the [StgProject] state.
     *
     * See [journalSpec] on why the tests re-create the framework-private specification.
     */
    fun stateHistorySpec(): RecordSpec<EntityStateKey, EntityRecord> =
        RecordSpec(
            StgProject::class.java,
            EntityStateKey::class.java,
            EntityRecord::class.java,
            EntityStateHistoryColumns.definitions()
        ) { record ->
            entityStateKey {
                entityId = record.entityId
                version = record.version.number
            }
        }

    /**
     * A test-only entity with the state type distinct from the one of
     * `JdbcStorageFactoryTestEnv.StgProjectAggregate`, for asserting that
     * the storages of different entity types stay apart.
     */
    internal class StgTaskEntity : AbstractEntity<StgTaskId, StgTask>()
}
