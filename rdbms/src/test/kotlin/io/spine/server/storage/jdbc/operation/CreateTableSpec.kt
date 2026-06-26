/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.spine.server.storage.jdbc.operation

import io.kotest.matchers.optional.shouldBePresent
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.spine.base.Identifier.newUuid
import io.spine.query.RecordColumn
import io.spine.server.storage.RecordSpec
import io.spine.server.storage.Storage
import io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4
import io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_9_7
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.singleTenantSpec
import io.spine.server.storage.jdbc.record.RecordTable
import io.spine.test.storage.StgProject
import io.spine.test.storage.StgProjectId
import io.spine.test.storage.stgProject
import io.spine.test.storage.stgProjectId
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Tests that [CreateTable] composes a valid `CREATE TABLE` statement: it escapes column and
 * table names so records with columns named after reserved SQL keywords can be stored, and it
 * applies a binary collation to MySQL string columns so identifiers stay case-sensitive.
 */
@DisplayName("`CreateTable` should")
internal class CreateTableSpec {

    @Test
    fun `escape a column named after a reserved SQL keyword in the generated SQL`() {
        val factory = h2Factory()
        val tableSpec = factory.tableSpecFor(specWithGroupColumn())
        val table = RecordTable.by(tableSpec, factory)

        val sql = table.creationSql()

        // H2 quotes the reserved identifiers with the double quote character.
        sql shouldContain "\"group\""
    }

    @Test
    fun `apply a binary collation to MySQL string columns to keep IDs case-sensitive`() {
        val factory = mysqlFactory()
        val tableSpec = factory.tableSpecFor(specWithGroupColumn())
        val table = RecordTable.by(tableSpec, factory)

        val sql = table.creationSql()

        // The ID column is stored as `VARCHAR(512)`, and the `group` column as `TEXT`; both
        // must carry the binary collation, otherwise MySQL compares them case-insensitively
        // and distinct identifiers like `"name"` and `"Name"` collide.
        sql shouldContain "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        sql shouldContain "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
    }

    @Test
    fun `create a table and store a record with a reserved-keyword column`() {
        val factory = h2Factory()

        // Before the fix, creating the storage failed with a `DatabaseException`, as the
        // generated `CREATE TABLE` statement referred to the unescaped `group` column.
        val storage: Storage<StgProjectId, StgProject> =
            factory.createRecordStorage(singleTenantSpec(), specWithGroupColumn())

        val projectId = stgProjectId { id = newUuid() }
        val project = stgProject {
            id = projectId
            name = "Engineering"
        }
        storage.write(projectId, project)

        val stored = storage.read(projectId).shouldBePresent()
        stored shouldBe project
    }

    companion object {

        /**
         * A column named after the `GROUP` SQL keyword, which is reserved in the H2, MySQL,
         * and PostgreSQL dialects, and as such must be escaped when used as a column name.
         *
         * The column reuses the `name` field of [StgProject] as its value, which is
         * irrelevant for the purpose of these tests.
         */
        private val group: RecordColumn<StgProject, String> =
            RecordColumn.create("group", String::class.java) { it.name }

        private fun h2Factory(): JdbcStorageFactory =
            JdbcStorageFactory.newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setTypeMapping(H2_2_4)
                .build()

        /**
         * A factory configured with the [MYSQL_9_7] type mapping over an in-memory data source.
         *
         * The data source merely backs the SQL composition (no `CREATE TABLE` is executed), so
         * the resulting DDL carries MySQL type names while staying free of a running MySQL server.
         */
        private fun mysqlFactory(): JdbcStorageFactory =
            JdbcStorageFactory.newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setTypeMapping(MYSQL_9_7)
                .build()

        private fun specWithGroupColumn(): RecordSpec<StgProjectId, StgProject> =
            RecordSpec(
                StgProjectId::class.java,
                StgProject::class.java,
                StgProject::getId,
                listOf(group)
            )
    }
}
