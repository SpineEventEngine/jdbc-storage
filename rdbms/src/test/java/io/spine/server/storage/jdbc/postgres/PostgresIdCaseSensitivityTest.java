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

package io.spine.server.storage.jdbc.postgres;

import io.spine.testing.SlowTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_10_1;
import static io.spine.server.storage.jdbc.Type.STRING_512;

/**
 * Verifies that PostgreSQL keeps identifiers that differ only by case distinct, using the exact
 * SQL type that the {@linkplain io.spine.server.storage.jdbc.PredefinedMapping#POSTGRESQL_10_1
 * predefined PostgreSQL mapping} produces for identifier columns.
 *
 * <p>Unlike MySQL — whose default non-binary collation is case-insensitive — PostgreSQL compares
 * strings case-sensitively by default: its default collations are deterministic, so equality is
 * byte-exact and case folding is never applied. The plain {@code VARCHAR} mapping therefore needs
 * no binary collation to keep {@code "name"} and {@code "Name"} apart, and this test asserts that
 * behavior against a real PostgreSQL server.
 */
@DisplayName("PostgreSQL, with the predefined mapping, should")
@SlowTest
@Testcontainers(disabledWithoutDocker = true)
final class PostgresIdCaseSensitivityTest {

    @Container
    private static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    @DisplayName("compare identifier columns case-sensitively without an explicit collation")
    void keepCaseDistinctIdentifiersDistinct() throws SQLException {
        // The SQL type the PostgreSQL mapping produces for identifier columns.
        var idType = POSTGRESQL_10_1.typeNameFor(STRING_512).value();

        try (var connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {

            statement.execute("CREATE TABLE entity (id " + idType + " PRIMARY KEY)");
            statement.executeUpdate("INSERT INTO entity(id) VALUES ('name')");
            // A case-insensitive primary key would reject this as a duplicate of `name`.
            statement.executeUpdate("INSERT INTO entity(id) VALUES ('Name')");

            try (var rows = statement.executeQuery("SELECT count(*) FROM entity")) {
                rows.next();
                // Both identifiers are stored as distinct rows: PostgreSQL keeps them apart by
                // default, so the plain `VARCHAR` mapping needs no binary collation.
                assertThat(rows.getInt(1)).isEqualTo(2);
            }
        }
    }
}
