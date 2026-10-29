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

    // The version is that of the MySQL server LTS release, which `select()` compares with
    // the version the server reports. It is not the version of the JDBC driver,
    // `io.spine.dependency.storage.MySql`. Must match the Docker image of `MysqlTests`,
    // as `MysqlServerVersionSpec` checks.
    //
    // MySQL compares non-binary string types case- and accent-insensitively by default, so
    // distinct identifiers like `"name"` and `"Name"` would collide. All character-based column
    // types therefore carry an explicit binary collation; see `MySqlTypeNames`.
    MYSQL_9_7("MySQL", 9, 7,
              mappingBuilder().add(STRING_255, MySqlTypeNames.VARCHAR_255)
                              .add(STRING_512, MySqlTypeNames.VARCHAR_512)
                              .add(STRING, MySqlTypeNames.TEXT)),

    // PostgreSQL has no bare `DOUBLE` type, and its `FLOAT` is double-precision;
    // map to the single-/double-precision types matching Java `float`/`double`.
    POSTGRESQL_10_1("PostgreSQL", 10, 1,
                    mappingBuilder().add(BYTE_ARRAY, "BYTEA")
                                    .add(FLOAT, PostgreSqlTypeNames.REAL)
                                    .add(DOUBLE, PostgreSqlTypeNames.DOUBLE_PRECISION)),

    // Must match `io.spine.dependency.storage.H2.version`.
    H2_2_5("H2", 2, 5, mappingBuilder());

    /**
     * A portable mapping used by {@link #select(DataSourceWrapper) select} for a database it does
     * not recognize.
     *
     * <p>It exposes the {@linkplain TypeMappingBuilder#mappingBuilder() default} type names with
     * no dialect-specific clauses — in particular, without the {@linkplain MySqlTypeNames MySQL
     * binary collation} — so that table creation does not emit DDL an unknown engine cannot parse.
     */
    private static final TypeMapping GENERIC_MAPPING = mappingBuilder().build();

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
     * @return the type mapping for the used database; a recognized product reported at an
     *         unlisted version still uses that product's mapping, and an unrecognized database
     *         uses the {@linkplain #GENERIC_MAPPING generic mapping} with portable type names
     */
    public static TypeMapping select(DataSourceWrapper dataSource) {
        checkNotNull(dataSource);
        var metaData = dataSource.metaData();
        PredefinedMapping sameProduct = null;
        for (var mapping : values()) {
            if (!metaData.productName().equals(mapping.databaseProductName)) {
                continue;
            }
            var versionMatch =
                    metaData.majorVersion() == mapping.majorVersion
                    && metaData.minorVersion() == mapping.minorVersion;
            if (versionMatch) {
                return mapping;
            }
            sameProduct = mapping;
        }
        // A recognized product reported at an unlisted version still uses that product's mapping,
        // as its dialect-specific type names (the MySQL binary collation, the PostgreSQL
        // `BYTEA`/`DOUBLE PRECISION`) apply across that product's versions. An unrecognized
        // database falls back to a generic mapping, whose portable type names carry no
        // dialect-specific clauses an unknown engine might not parse.
        return sameProduct != null ? sameProduct : GENERIC_MAPPING;
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
}
