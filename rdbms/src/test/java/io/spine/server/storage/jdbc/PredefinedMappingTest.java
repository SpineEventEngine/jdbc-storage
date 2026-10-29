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

package io.spine.server.storage.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.jdbc.GivenDataSource.whichHoldsMetadata;
import static io.spine.server.storage.jdbc.PostgreSqlTypeNames.DOUBLE_PRECISION;
import static io.spine.server.storage.jdbc.PostgreSqlTypeNames.REAL;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_5;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_9_7;
import static io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_18;
import static io.spine.server.storage.jdbc.PredefinedMapping.select;
import static io.spine.server.storage.jdbc.Type.DOUBLE;
import static io.spine.server.storage.jdbc.Type.FLOAT;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.server.storage.jdbc.Type.STRING_512;
import static io.spine.testing.TestValues.nullRef;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`PredefinedMapping` should")
class PredefinedMappingTest {

    private static final PredefinedMapping mapping = POSTGRESQL_18;

    @Test
    @DisplayName("throw ISE if requested type has no mapping")
    void throwOnNoMapping() {
        Type notMappedType = nullRef();
        assertThrows(IllegalStateException.class, () -> MYSQL_9_7.typeNameFor(notMappedType));
    }

    @Test
    @DisplayName("be selected by database product name and major version")
    void selectTypeMapping() {
        // A patch release of the major version the mapping is made for.
        var dataSource = whichHoldsMetadata(mapping.getDatabaseProductName(),
                                            mapping.getMajorVersion(),
                                            6);
        assertThat(select(dataSource))
                .isEqualTo(mapping);
    }

    @Test
    @DisplayName("select a product's mapping for an unlisted version of that product")
    void selectByProductNameForUnlistedVersion() {
        var newMajorVersion = mapping.getMajorVersion() + 1;
        var dataSource = whichHoldsMetadata(mapping.getDatabaseProductName(),
                                            newMajorVersion,
                                            0);
        // A recognized product at an unlisted version uses that product's dialect mapping,
        // rather than the generic fallback meant for unknown databases.
        assertThat(select(dataSource))
                .isEqualTo(mapping);
    }

    @Test
    @DisplayName("map `FLOAT` and `DOUBLE` to default SQL type names")
    void mapFloatingPointTypes() {
        assertThat(MYSQL_9_7.typeNameFor(FLOAT).value()).isEqualTo("FLOAT");
        assertThat(MYSQL_9_7.typeNameFor(DOUBLE).value()).isEqualTo("DOUBLE");
    }

    @Test
    @DisplayName("map `FLOAT` and `DOUBLE` to PostgreSQL-specific SQL type names")
    void mapFloatingPointTypesForPostgres() {
        assertThat(POSTGRESQL_18.typeNameFor(FLOAT).value()).isEqualTo(REAL);
        assertThat(POSTGRESQL_18.typeNameFor(DOUBLE).value()).isEqualTo(DOUBLE_PRECISION);
    }

    @Test
    @DisplayName("map `String` types to case-sensitive binary collations for MySQL")
    void mapStringTypesForMysqlToBinaryCollation() {
        assertThat(MYSQL_9_7.typeNameFor(STRING_255).value())
                .isEqualTo("VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
        assertThat(MYSQL_9_7.typeNameFor(STRING_512).value())
                .isEqualTo("VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
        assertThat(MYSQL_9_7.typeNameFor(STRING).value())
                .isEqualTo("TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
    }

    @Test
    @DisplayName("not apply the MySQL binary collation to other databases")
    void keepStringTypesPlainForOtherDatabases() {
        for (var caseSensitiveDb : new PredefinedMapping[]{POSTGRESQL_18, H2_2_5}) {
            assertThat(caseSensitiveDb.typeNameFor(STRING_255).value()).isEqualTo("VARCHAR(255)");
            assertThat(caseSensitiveDb.typeNameFor(STRING_512).value()).isEqualTo("VARCHAR(512)");
            assertThat(caseSensitiveDb.typeNameFor(STRING).value()).isEqualTo("TEXT");
        }
    }

    @Test
    @DisplayName("keep the MySQL mapping for a MySQL server of an unlisted version")
    void selectMysqlMappingForOtherMysqlVersion() {
        var mysqlOtherVersion = whichHoldsMetadata(MYSQL_9_7.getDatabaseProductName(), 8, 0);
        // The binary collation is valid across MySQL versions, so the fix still applies.
        assertThat(select(mysqlOtherVersion)).isEqualTo(MYSQL_9_7);
    }

    @Test
    @DisplayName("fall back to portable type names for an unrecognized database")
    void fallBackToPortableTypesForUnknownDatabase() {
        var unknownDb = whichHoldsMetadata("AcmeDB", 1, 0);
        var fallback = select(unknownDb);
        // The MySQL-only collation must not leak into the DDL for a database that cannot parse it.
        assertThat(fallback).isNotEqualTo(MYSQL_9_7);
        assertThat(fallback.typeNameFor(STRING_512).value()).isEqualTo("VARCHAR(512)");
        assertThat(fallback.typeNameFor(STRING).value()).isEqualTo("TEXT");
    }
}
