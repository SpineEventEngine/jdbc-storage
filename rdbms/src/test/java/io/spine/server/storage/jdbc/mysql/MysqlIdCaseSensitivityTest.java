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

package io.spine.server.storage.jdbc.mysql;

import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProjectId;
import io.spine.testing.SlowTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.singleTenantSpec;

/**
 * Verifies that identifiers are stored case-sensitively on MySQL.
 *
 * <p>MySQL compares non-binary string types case-insensitively by default. Entity identifiers
 * are stored in a {@code VARCHAR} column, so without an explicit binary collation two
 * identifiers differing only by case — such as a username {@code "name"} versus
 * {@code "Name"} — would map to the same row, and commands addressed to one entity would
 * reach the other.
 *
 * @see io.spine.server.storage.jdbc.PredefinedMapping#MYSQL_9_7
 */
@DisplayName("`JdbcRecordStorage` running on top of MySQL instance should")
@SlowTest
@EnableConditionally
final class MysqlIdCaseSensitivityTest {

    @Test
    @DisplayName("not let identifiers differing only by case collide")
    void notCollideOnIdCase() {
        var factory = MysqlTests.newFactory();
        RecordStorage<StgProjectId, StgProject> storage =
                factory.createRecordStorage(singleTenantSpec(), projectSpec());

        var lowerId = projectId("name");
        var upperId = projectId("Name");
        var lowerCase = project(lowerId, "Lower-case project");
        var upperCase = project(upperId, "Upper-case project");

        storage.write(lowerId, lowerCase);
        storage.write(upperId, upperCase);

        // Each identifier must address its own record. Under MySQL's default case-insensitive
        // collation both writes would target a single row, so the second would overwrite the
        // first and both reads would return `upperCase`.
        assertThat(storage.read(lowerId)).hasValue(lowerCase);
        assertThat(storage.read(upperId)).hasValue(upperCase);
    }

    private static RecordSpec<StgProjectId, StgProject> projectSpec() {
        return new RecordSpec<>(StgProjectId.class, StgProject.class, StgProject::getId);
    }

    private static StgProjectId projectId(String value) {
        return StgProjectId.newBuilder()
                .setId(value)
                .build();
    }

    private static StgProject project(StgProjectId id, String name) {
        return StgProject.newBuilder()
                .setId(id)
                .setName(name)
                .build();
    }
}
