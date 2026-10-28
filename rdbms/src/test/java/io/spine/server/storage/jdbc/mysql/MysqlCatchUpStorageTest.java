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

package io.spine.server.storage.jdbc.mysql;

import io.spine.server.delivery.CatchUpStorage;
import io.spine.server.delivery.CatchUpStorageTest;
import io.spine.testing.SlowTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

/**
 * Runs the {@link CatchUpStorageTest} suite against a MySQL instance.
 *
 * <p>The suite checks that the catch-up jobs can be queried by their columns on a real RDBMS.
 * It is meant to be run locally only. Unlike the other tests of this package, it is skipped
 * on CI, which GitHub Actions marks with the {@code CI} environment variable.
 */
@DisplayName("`CatchUpStorage` running on top of MySQL instance should")
@SlowTest
@EnableConditionally
@DisabledIfEnvironmentVariable(named = "CI", matches = "true",
                               disabledReason = "The suite is meant to be run locally only.")
class MysqlCatchUpStorageTest extends CatchUpStorageTest {

    @Override
    protected CatchUpStorage newStorage() {
        return MysqlTests.newFactory()
                         .createCatchUpStorage(false);
    }
}
