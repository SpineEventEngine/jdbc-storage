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
import io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_9_7
import io.spine.server.storage.jdbc.mysql.EnableConditionally
import io.spine.server.storage.jdbc.mysql.MysqlTests
import io.spine.testing.SlowTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Checks that the MySQL tests run against the server release of the [MYSQL_9_7] mapping.
 *
 * [PredefinedMapping.select] compares the version of a mapping with the version the server
 * reports, so the version of [MYSQL_9_7] refers to the server, not to the JDBC driver.
 * Without this check, the tests could run against another server release than the one
 * the mapping is made for.
 *
 * The spec resides in the package of [PredefinedMapping] to read the version of a mapping.
 */
@DisplayName("The MySQL server used in the tests should")
@SlowTest
@EnableConditionally
internal class MysqlServerVersionSpec {

    @Test
    fun `report the version of the 'MYSQL_9_7' mapping`() {
        val metaData = MysqlTests.newFactory()
            .dataSource()
            .metaData()

        metaData.productName() shouldBe MYSQL_9_7.databaseProductName
        metaData.majorVersion() shouldBe MYSQL_9_7.majorVersion
        metaData.minorVersion() shouldBe MYSQL_9_7.minorVersion
    }
}
