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

package io.spine.server.storage.jdbc.record

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.spine.base.Identifier
import io.spine.core.Event
import io.spine.core.EventId
import io.spine.query.RecordQuery
import io.spine.server.entity.EntityStateKey
import io.spine.server.entity.storage.SpecScanner
import io.spine.server.storage.RecordSpec
import io.spine.server.storage.StorageGroup
import io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.StgProjectAggregate
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.StgTaskEntity
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.journalSpec
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.stateHistorySpec
import io.spine.test.storage.StgProjectId
import io.spine.test.storage.event.StgProjectCreated
import io.spine.test.storage.stgProjectId
import io.spine.testdata.Sample
import io.spine.testing.server.TestEventFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Tests that [JdbcStorageFactory] allocates a distinct table per the combination
 * of a record specification and a [StorageGroup], as the framework expects
 * of storage vendors.
 *
 * Without the group taking part in the table identity, the storages holding records
 * of the same type would conflate: the event journals of all entity types — and
 * the event log of the Bounded Context — store `Event`s, and the state history
 * of an entity type stores `EntityRecord`s, just as its latest-state storage does.
 */
@DisplayName("`JdbcStorageFactory`, when allocating grouped tables, should")
internal class GroupedTableAllocationSpec {

    private lateinit var factory: JdbcStorageFactory

    private val projectGroup = StorageGroup.of(StgProjectAggregate::class.java)
    private val taskGroup = StorageGroup.of(StgTaskEntity::class.java)

    @BeforeEach
    fun createFactory() {
        factory = h2Factory()
    }

    @AfterEach
    fun closeFactory() {
        if (this::factory.isInitialized && factory.isOpen) {
            factory.close()
        }
    }

    @Test
    fun `tell apart the latest state, event journal, and state history of an entity type`() {
        val latestState = factory.tableSpecFor(SpecScanner.scan(StgProjectAggregate::class.java))
        val journal = factory.tableSpecFor(journalSpec(), projectGroup)
        val stateHistory = factory.tableSpecFor(stateHistorySpec(), projectGroup)

        latestState.tableName() shouldBe "spine_test_storage_StgProject"
        journal.tableName() shouldBe "spine_test_storage_StgProject_Event"
        stateHistory.tableName() shouldBe "spine_test_storage_StgProject_EntityRecord"
    }

    @Test
    fun `key the ID column of each table by the identifier type of its own records`() {
        // Before the group-aware allocation, the table specifications were cached
        // by the source type of the record specification alone. The state history
        // of an entity type — sharing the source type with the latest-state storage —
        // would receive the specification of the latter, with the ID column
        // of the entity identifier type instead of `EntityStateKey`.
        val latestState = factory.tableSpecFor(SpecScanner.scan(StgProjectAggregate::class.java))
        val stateHistory = factory.tableSpecFor(stateHistorySpec(), projectGroup)

        latestState.idColumn().javaType() shouldBe StgProjectId::class.java
        stateHistory.idColumn().javaType() shouldBe EntityStateKey::class.java
    }

    @Test
    fun `allocate distinct tables to the event journals of different entity types`() {
        val projectJournal = factory.tableSpecFor(journalSpec(), projectGroup)
        val taskJournal = factory.tableSpecFor(journalSpec(), taskGroup)

        projectJournal.tableName() shouldBe "spine_test_storage_StgProject_Event"
        taskJournal.tableName() shouldBe "spine_test_storage_StgTask_Event"
    }

    @Test
    fun `keep the events of an entity type out of the journals of other types`() {
        val context = HistoryStorageTestEnv.context()
        val projectJournal =
            factory.createEntityEventStorage(context, StgProjectAggregate::class.java)
        val taskJournal = factory.createEntityEventStorage(context, StgTaskEntity::class.java)

        projectJournal.write(newEvent())

        // Read each journal in full, without filtering by an entity,
        // to observe the whole underlying table.
        val everythingJournaled =
            RecordQuery.newBuilder(EventId::class.java, Event::class.java)
                .build()
        taskJournal.readAll(everythingJournaled)
            .asSequence()
            .toList()
            .shouldBeEmpty()
        projectJournal.readAll(everythingJournaled)
            .asSequence()
            .toList() shouldHaveSize 1
    }

    @Test
    fun `serve one physical table to the repeatedly created storages of one group`() {
        val context = HistoryStorageTestEnv.context()
        val first = factory.createEntityEventStorage(context, StgProjectAggregate::class.java)
        val second = factory.createEntityEventStorage(context, StgProjectAggregate::class.java)
        val event = newEvent()

        first.write(event)

        second.historyBackward(producerOf(event), batchSize = 1)
            .asSequence()
            .toList() shouldContainExactly listOf(event)
    }

    @Test
    fun `apply a custom table name only to the storages outside any group`() {
        factory.close()
        factory = JdbcStorageFactory.newBuilder()
            .setDataSource(whichIsStoredInMemory(Identifier.newUuid()))
            .setTypeMapping(H2_2_4)
            .setTableName(Event::class.java, "custom_event_log")
            .build()

        val ungroupedEvents = RecordSpec(
            EventId::class.java,
            Event::class.java
        ) { event -> event.id }
        val eventLog = factory.tableSpecFor(ungroupedEvents)
        val journal = factory.tableSpecFor(journalSpec(), projectGroup)

        eventLog.tableName() shouldBe "custom_event_log"
        journal.tableName() shouldBe "spine_test_storage_StgProject_Event"
    }

    private fun newEvent(): Event {
        val producer = stgProjectId {
            id = "grouped-tables-entity"
        }
        val eventFactory = TestEventFactory.newInstance(
            Identifier.pack(producer),
            GroupedTableAllocationSpec::class.java
        )
        return eventFactory.createEvent(Sample.messageOfType(StgProjectCreated::class.java))
    }

    private fun producerOf(event: Event): StgProjectId =
        Identifier.unpack(event.context.producerId, StgProjectId::class.java)
}
