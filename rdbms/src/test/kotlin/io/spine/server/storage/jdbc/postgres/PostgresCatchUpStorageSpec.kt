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

package io.spine.server.storage.jdbc.postgres

import io.spine.server.delivery.CatchUpStorage
import io.spine.server.delivery.CatchUpStorageTest
import io.spine.testing.SlowTest
import org.junit.jupiter.api.DisplayName
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Runs the [CatchUpStorageTest] suite against a PostgreSQL instance.
 *
 * The suite checks that the catch-up jobs can be queried by their columns
 * on a PostgreSQL server.
 */
@DisplayName("`CatchUpStorage` running on top of PostgreSQL instance should")
@SlowTest
@Testcontainers(disabledWithoutDocker = true)
internal class PostgresCatchUpStorageSpec : CatchUpStorageTest() {

    override fun newStorage(): CatchUpStorage =
        PostgresTests.newFactory()
            .createCatchUpStorage(false)
}
