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

package io.spine.server.storage.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.jdbc.GivenDataSource.whichHoldsMetadata;
import static io.spine.server.storage.jdbc.PostgreSqlTypeNames.DOUBLE_PRECISION;
import static io.spine.server.storage.jdbc.PostgreSqlTypeNames.REAL;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_9_7;
import static io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_10_1;
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

    private static final PredefinedMapping mapping = POSTGRESQL_10_1;

    @Test
    @DisplayName("throw ISE if requested type has no mapping")
    void throwOnNoMapping() {
        Type notMappedType = nullRef();
        assertThrows(IllegalStateException.class, () -> MYSQL_9_7.typeNameFor(notMappedType));
    }

    @Test
    @DisplayName("be selected by database product name and major version")
    void selectTypeMapping() {
        var dataSource = whichHoldsMetadata(mapping.getDatabaseProductName(),
                                            mapping.getMajorVersion(),
                                            mapping.getMinorVersion());
        assertThat(select(dataSource))
                .isEqualTo(mapping);
    }

    @Test
    @DisplayName("select a product's mapping for an unlisted version of that product")
    void selectByProductNameForUnlistedVersion() {
        var newMajorVersion = mapping.getMajorVersion() + 1;
        var dataSource = whichHoldsMetadata(mapping.getDatabaseProductName(),
                                            newMajorVersion,
                                            mapping.getMinorVersion());
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
        assertThat(POSTGRESQL_10_1.typeNameFor(FLOAT).value()).isEqualTo(REAL);
        assertThat(POSTGRESQL_10_1.typeNameFor(DOUBLE).value()).isEqualTo(DOUBLE_PRECISION);
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
        for (var caseSensitiveDb : new PredefinedMapping[]{POSTGRESQL_10_1, H2_2_4}) {
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
