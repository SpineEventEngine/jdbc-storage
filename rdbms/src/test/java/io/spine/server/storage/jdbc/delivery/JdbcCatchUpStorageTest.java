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

import io.spine.server.delivery.CatchUpStorage;
import io.spine.server.delivery.CatchUpStorageTest;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;

import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;

@DisplayName("JDBC-based `CatchUpStorage` should")
class JdbcCatchUpStorageTest extends CatchUpStorageTest {

    private JdbcStorageFactory factory;

    @Override
    protected CatchUpStorage newStorage() {
        factory = newFactory();
        return factory.createCatchUpStorage(false);
    }

    @AfterEach
    void closeFactory() {
        factory.close();
    }
}
