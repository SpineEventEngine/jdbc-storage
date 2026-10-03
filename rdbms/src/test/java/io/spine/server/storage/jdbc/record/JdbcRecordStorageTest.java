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

package io.spine.server.storage.jdbc.record;

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.storage.DelegatingRecordStorageTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;

@DisplayName("`JdbcRecordStorage` should")
class JdbcRecordStorageTest extends DelegatingRecordStorageTest {

    @BeforeEach
    @Override
    protected void setUpAbstractStorageTest() {
        ServerEnvironment.under(Tests.class)
                         .useStorageFactory((env) -> newFactory());
        super.setUpAbstractStorageTest();
    }

    @AfterAll
    static void tearDownClass() {
        ServerEnvironment.instance().reset();
    }
}
