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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.spine.type.TypeName;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.DOUBLE;
import static io.spine.server.storage.jdbc.Type.FLOAT;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.server.storage.jdbc.Type.STRING_512;
import static io.spine.server.storage.jdbc.TypeMappingBuilder.mappingBuilder;

/**
 * Predefined {@linkplain TypeMapping type mappings} for different databases.
 */
@Immutable
public enum PredefinedMapping implements TypeMapping {

    // Must match `io.spine.dependency.storage.MySql.version`.
    //
    // MySQL compares non-binary string types case- and accent-insensitively by default, so
    // distinct identifiers like `"name"` and `"Name"` would collide. All character-based column
    // types therefore carry an explicit binary collation; see the `MySql` helper below.
    MYSQL_9_7("MySQL", 9, 7, mappingBuilder().add(STRING_255, MySql.VARCHAR_255)
                                             .add(STRING_512, MySql.VARCHAR_512)
                                             .add(STRING, MySql.TEXT)),

    // PostgreSQL has no bare `DOUBLE` type, and its `FLOAT` is double-precision;
    // map to the single-/double-precision types matching Java `float`/`double`.
    POSTGRESQL_10_1("PostgreSQL", 10, 1, mappingBuilder().add(BYTE_ARRAY, "BYTEA")
                                                         .add(FLOAT, PostgreSql.REAL)
                                                         .add(DOUBLE, PostgreSql.DOUBLE_PRECISION)),

    // Must match `io.spine.dependency.storage.H2.version`.
    H2_2_4("H2", 2, 4, mappingBuilder());

    private final TypeMapping typeMapping;
    private final String databaseProductName;
    private final int majorVersion;
    private final int minorVersion;

    PredefinedMapping(String databaseProductName, int majorVersion,
                      int minorVersion, TypeMappingBuilder typeMappingBuilder) {
        this.databaseProductName = databaseProductName;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.typeMapping = typeMappingBuilder.build();
    }

    @Override
    public TypeName typeNameFor(Type type) {
        return typeMapping.typeNameFor(type);
    }

    /**
     * Selects the type mapping for the specified data source.
     *
     * <p>The {@linkplain DataSourceMetaData#productName() database product name} and the version
     * are taken into account during the selection.
     *
     * @param dataSource
     *         the data source to test suitability
     * @return the type mapping for the used database or {@linkplain PredefinedMapping#MYSQL_9_7
     *         mapping for MySQL 9.7} if there is no standard mapping for the database
     */
    public static TypeMapping select(DataSourceWrapper dataSource) {
        checkNotNull(dataSource);
        var metaData = dataSource.metaData();
        for (var mapping : values()) {
            var nameMatch = metaData.productName()
                                    .equals(mapping.databaseProductName);
            var versionMatch =
                    metaData.majorVersion() == mapping.majorVersion
                    && metaData.minorVersion() == mapping.minorVersion;
            if (nameMatch && versionMatch) {
                return mapping;
            }
        }
        return MYSQL_9_7;
    }

    @VisibleForTesting
    String getDatabaseProductName() {
        return databaseProductName;
    }

    @VisibleForTesting
    int getMajorVersion() {
        return majorVersion;
    }

    @VisibleForTesting
    int getMinorVersion() {
        return minorVersion;
    }

    /**
     * SQL type names specific to MySQL, which differ from the
     * {@linkplain TypeMappingBuilder default mapping}.
     */
    static final class MySql {

        /**
         * The character set and binary collation appended to every character-based column type.
         *
         * <p>By default, MySQL uses a case- and accent-insensitive collation for non-binary
         * string types, so {@code 'name'} and {@code 'Name'} compare as equal. Entity
         * identifiers and {@code String} columns must be matched exactly; otherwise distinct
         * identifiers collide and commands for one entity get routed to another. A binary
         * collation restores exact, case-sensitive matching.
         *
         * <p>{@code utf8mb4} (rather than the deprecated {@code utf8}/{@code utf8mb3}) is used to
         * keep the full Unicode range available. The collation does not change the stored byte
         * width, so the {@code VARCHAR(512)} primary key stays within InnoDB index limits.
         */
        private static final String BINARY = "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";

        /** {@code VARCHAR(255)} with a {@linkplain #BINARY binary collation}. */
        static final String VARCHAR_255 = "VARCHAR(255) " + BINARY;

        /** {@code VARCHAR(512)} with a {@linkplain #BINARY binary collation}. */
        static final String VARCHAR_512 = "VARCHAR(512) " + BINARY;

        /** {@code TEXT} with a {@linkplain #BINARY binary collation}. */
        static final String TEXT = "TEXT " + BINARY;

        private MySql() {
        }
    }

    /**
     * SQL type names specific to PostgreSQL, which differ from the
     * {@linkplain TypeMappingBuilder default mapping}.
     */
    static final class PostgreSql {

        /** The single-precision (4-byte) floating-point type. */
        static final String REAL = "REAL";

        /**
         * The double-precision (8-byte) floating-point type.
         *
         * <p>Used because PostgreSQL does not recognize a bare {@code DOUBLE}.
         */
        static final String DOUBLE_PRECISION = "DOUBLE PRECISION";

        private PostgreSql() {
        }
    }
}
