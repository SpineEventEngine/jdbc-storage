/*
 * Copyright 2026 CodeMatters, Lda.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package io.spine.server.storage.jdbc

import io.spine.environment.Tests
import io.spine.server.ServerEnvironment
import io.spine.server.event.store.DefaultEventStoreTest
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.DisplayName

/**
 * Runs the [DefaultEventStoreTest] contract against [JdbcStorageFactory]
 * over an in-memory H2 database.
 *
 * The base suite builds a Bounded Context in its `@BeforeEach`, which runs
 * before any `@BeforeEach` of this class could. JUnit creates a new test
 * instance per test method, so the constructor of this class is the only
 * seam ahead of the base setup: it points the test server environment to
 * a fresh storage factory — and thus a fresh database — for every test,
 * keeping the persistent event tables from leaking between the tests.
 */
@DisplayName("JDBC-backed `EventStore` should")
internal class JdbcDefaultEventStoreTest : DefaultEventStoreTest() {

    init {
        ServerEnvironment.under(Tests::class.java).use(freshFactory())
    }

    companion object {

        private val factories = mutableListOf<JdbcStorageFactory>()

        /**
         * Creates a new factory over a fresh in-memory H2 database,
         * remembering it for the after-all cleanup.
         */
        private fun freshFactory(): JdbcStorageFactory =
            h2Factory().also { factories.add(it) }

        /**
         * Detaches the storage configuration from the test server environment
         * and closes the factories created for the test methods.
         */
        @AfterAll
        @JvmStatic
        fun resetEnvironment() {
            ServerEnvironment.instance().reset()
            factories.forEach(JdbcStorageFactory::close)
            factories.clear()
        }
    }
}
