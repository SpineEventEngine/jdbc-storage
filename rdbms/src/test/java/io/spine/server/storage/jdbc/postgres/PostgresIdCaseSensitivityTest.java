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

package io.spine.server.storage.jdbc.postgres;

import io.spine.testing.SlowTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

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
    private static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:16-alpine");

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
