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

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.spine.base.Identifier
import io.spine.base.Time.currentTime
import io.spine.core.Versions
import io.spine.protobuf.AnyPacker
import io.spine.server.entity.entityRecord
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.StgProjectAggregate
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.StgTaskEntity
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import io.spine.test.storage.stgProject
import io.spine.test.storage.stgProjectId
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Tests that [JdbcStorageFactory] tolerates the concurrent creation of the storages
 * sharing a table.
 *
 * The framework may create a state history storage lazily — upon the first dispatch
 * to a recording repository, on a delivery worker thread — concurrently with
 * the storage creation of other repositories. See the API note of
 * `StorageFactory.createEntityStateHistoryStorage`.
 */
@DisplayName("`JdbcStorageFactory`, on concurrent storage creation, should")
internal class ConcurrentHistoryCreationSpec {

    private lateinit var factory: JdbcStorageFactory

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
    fun `create each storage successfully, converging on one table per storage group`() {
        val context = HistoryStorageTestEnv.context()
        val threads = 8
        val started = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(threads)
        try {
            val creations = (1..threads).map { index ->
                executor.submit<Any> {
                    started.await()
                    if (index % 2 == 0) {
                        factory.createEntityStateHistoryStorage(
                            context, StgProjectAggregate::class.java
                        )
                    } else {
                        factory.createEntityEventStorage(context, StgTaskEntity::class.java)
                    }
                }
            }
            started.countDown()

            // Each creation either succeeds or the test fails with the thrown cause.
            creations.forEach { it.get(30, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }

        // The concurrently created storages converge on one physical table:
        // a record written through a fresh storage instance is visible to another.
        val entityId = stgProjectId { id = "concurrently-tracked" }
        val record = entityRecord {
            this.entityId = Identifier.pack(entityId)
            state = AnyPacker.pack(stgProject { id = entityId })
            version = Versions.newVersion(1, currentTime())
        }
        val writer = factory.createEntityStateHistoryStorage(
            context, StgProjectAggregate::class.java
        )
        val reader = factory.createEntityStateHistoryStorage(
            context, StgProjectAggregate::class.java
        )
        writer.write(record)
        reader.historyBackward(entityId, batchSize = 1)
            .asSequence()
            .toList() shouldContainExactly listOf(record)
        reader.stateAt(entityId, currentTime()) shouldBe record
    }
}
