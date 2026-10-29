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
import io.spine.server.storage.jdbc.GivenDataSource.whichHoldsMetadata
import io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_9_7
import io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_18
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * The Kotlin-side tests of [PredefinedMapping], and the place for its new tests.
 *
 * Supplements [PredefinedMappingTest], written in Java.
 *
 * @see PredefinedMappingTest
 */
@DisplayName("`PredefinedMapping` should")
internal class PredefinedMappingSpec {

    @Test
    fun `match any patch release of the PostgreSQL major version it is made for`() {
        POSTGRESQL_18.matches(18, 0) shouldBe true
        POSTGRESQL_18.matches(18, 6) shouldBe true
    }

    @Test
    fun `not match another PostgreSQL major version`() {
        POSTGRESQL_18.matches(17, 6) shouldBe false
        POSTGRESQL_18.matches(19, 0) shouldBe false
    }

    @Test
    fun `match only the MySQL release it is made for`() {
        MYSQL_9_7.matches(9, 7) shouldBe true
        MYSQL_9_7.matches(9, 6) shouldBe false
        MYSQL_9_7.matches(8, 7) shouldBe false
    }
}

/**
 * Tells whether this mapping is made for the given version of its database.
 */
private fun PredefinedMapping.matches(majorVersion: Int, minorVersion: Int): Boolean {
    val dataSource = whichHoldsMetadata(databaseProductName, majorVersion, minorVersion)
    return matchesVersionOf(dataSource.metaData())
}
