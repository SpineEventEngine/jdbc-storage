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

package io.spine.server.storage.jdbc.operation;

import com.google.common.collect.ImmutableList;
import io.spine.query.RecordColumn;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.Storage;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.singleTenantSpec;

/**
 * Tests that {@link CreateTable} escapes the names of the columns and tables,
 * so that records with columns named after reserved SQL keywords may be stored.
 *
 * @see <a href="https://github.com/SpineEventEngine/jdbc-storage/issues/172">Issue #172</a>
 */
@DisplayName("`CreateTable` should")
class CreateTableTest {

    /**
     * A column named after the {@code GROUP} SQL keyword, which is reserved in the H2, MySQL,
     * and PostgreSQL dialects, and as such must be escaped when used as a column name.
     *
     * <p>The column reuses the {@code name} field of {@code StgProject} as its value, which is
     * irrelevant for the purpose of these tests.
     */
    private static final RecordColumn<StgProject, String> GROUP =
            RecordColumn.create("group", String.class, StgProject::getName);

    @Test
    @DisplayName("escape a column named after a reserved SQL keyword in the generated SQL")
    void escapeReservedKeywordInSql() {
        var factory = h2Factory();
        var tableSpec = factory.tableSpecFor(specWithGroupColumn());
        var table = RecordTable.by(tableSpec, factory);

        var sql = table.creationSql();

        // H2 quotes the reserved identifiers with the double quote character.
        assertThat(sql)
                .contains("\"group\"");
    }

    @Test
    @DisplayName("create a table and store a record with a column named after a reserved SQL keyword")
    void storeRecordWithReservedKeywordColumn() {
        var factory = h2Factory();

        // Before the fix, creating the storage failed with a `DatabaseException`, as the
        // generated `CREATE TABLE` statement referred to the unescaped `group` column.
        Storage<StgProjectId, StgProject> storage =
                factory.createRecordStorage(singleTenantSpec(), specWithGroupColumn());

        var id = StgProjectId.newBuilder()
                .setId(newUuid())
                .build();
        var project = StgProject.newBuilder()
                .setId(id)
                .setName("Engineering")
                .build();
        storage.write(id, project);

        var read = storage.read(id);
        assertThat(read)
                .isPresent();
        assertThat(read.get())
                .isEqualTo(project);
    }

    private static JdbcStorageFactory h2Factory() {
        return JdbcStorageFactory.newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setTypeMapping(H2_2_4)
                .build();
    }

    private static RecordSpec<StgProjectId, StgProject> specWithGroupColumn() {
        return new RecordSpec<>(
                StgProjectId.class,
                StgProject.class,
                StgProject::getId,
                ImmutableList.of(GROUP));
    }
}
