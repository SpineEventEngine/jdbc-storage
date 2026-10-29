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

import io.kotest.matchers.shouldBe
import io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_18
import io.spine.server.storage.jdbc.postgres.PostgresTests
import io.spine.testing.SlowTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Checks that the PostgreSQL tests run against the major release of the [POSTGRESQL_18] mapping.
 *
 * [PredefinedMapping.select] compares the version of a mapping with the version the server
 * reports, so the version of [POSTGRESQL_18] refers to the server, not to the JDBC driver.
 * The minor version is not compared: PostgreSQL counts its patch releases with it.
 *
 * The spec resides in the package of [PredefinedMapping] to read the version of a mapping.
 */
@DisplayName("The PostgreSQL server used in the tests should")
@SlowTest
@Testcontainers(disabledWithoutDocker = true)
internal class PostgresServerVersionSpec {

    @Test
    fun `report the major version of the 'POSTGRESQL_18' mapping`() {
        val metaData = PostgresTests.newFactory()
            .dataSource()
            .metaData()

        metaData.productName() shouldBe POSTGRESQL_18.databaseProductName
        metaData.majorVersion() shouldBe POSTGRESQL_18.majorVersion
    }
}
