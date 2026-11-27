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

import io.spine.server.event.store.DefaultEventStoreTest
import io.spine.server.storage.StorageFactory
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import org.junit.jupiter.api.DisplayName

/**
 * Runs the [DefaultEventStoreTest] contract against a [JdbcStorageFactory]
 * over an in-memory H2 database.
 *
 * The application the base suite builds closes the factory after the test.
 */
@DisplayName("JDBC-backed `EventStore` should")
internal class JdbcDefaultEventStoreTest : DefaultEventStoreTest() {

    private var factory: JdbcStorageFactory? = null

    override fun storageFactory(): StorageFactory {
        val existing = factory
        if (existing != null) {
            return existing
        }
        return h2Factory().also { factory = it }
    }
}
