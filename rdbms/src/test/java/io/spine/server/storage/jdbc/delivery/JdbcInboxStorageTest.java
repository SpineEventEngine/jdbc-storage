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

package io.spine.server.storage.jdbc.delivery;

import io.spine.server.delivery.InboxStorageTest;
import io.spine.server.storage.StorageFactory;
import org.junit.jupiter.api.DisplayName;

import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;

/**
 * Runs the {@link InboxStorageTest} contract against a JDBC factory
 * over an in-memory H2 database.
 */
@DisplayName("JDBC-based `InboxStorage` should")
class JdbcInboxStorageTest extends InboxStorageTest {

    private StorageFactory factory;

    @Override
    protected StorageFactory storageFactory() {
        if (factory == null) {
            factory = newFactory();
        }
        return factory;
    }

    /**
     * Closes the factory after the storage.
     *
     * <p>The base suite closes the storage and leaves the factory open.
     */
    @Override
    protected void tearDownAbstractStorageTest() {
        super.tearDownAbstractStorageTest();
        if (factory == null) {
            return;
        }
        try {
            factory.close();
        } catch (RuntimeException e) {
            throw new IllegalStateException("Cannot close the storage factory.", e);
        }
    }
}
