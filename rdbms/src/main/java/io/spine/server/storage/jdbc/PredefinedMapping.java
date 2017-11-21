/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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
import io.spine.type.TypeName;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.MappingBuilder.basicBuilder;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;

/**
 * Predefined {@linkplain TypeMapping type mappings} for different databases.
 *
 * @author Dmytro Grankin
 */
public enum PredefinedMapping implements TypeMapping {

    MYSQL_5_7("MySQL", 5, 7, basicBuilder()),
    POSTGRESQL_10_1("PostgreSQL", 10, 1, basicBuilder().add(BYTE_ARRAY, "BYTEA"));

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final TypeMapping typeMapping;
    private final String databaseProductName;
    private final int majorVersion;
    private final int minorVersion;

    PredefinedMapping(String databaseProductName, int majorVersion,
                      int minorVersion, MappingBuilder mappingBuilder) {
        this.databaseProductName = databaseProductName;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.typeMapping = mappingBuilder.build();
    }

    @Override
    public TypeName typeNameFor(Type type) {
        return typeMapping.typeNameFor(type);
    }

    /**
     * Selects the type mapping for the specified data source.
     *
     * <p>The {@linkplain DatabaseMetaData#getDatabaseProductName() database product name} and
     * the version are taken into account during the selection.
     *
     * @param dataSource the data source to test suitability
     * @return the type mapping for the used database or {@linkplain PredefinedMapping#MYSQL_5_7
     *         mapping for MySQL 5.7} if there is no standard mapping for the database
     */
    static TypeMapping select(DataSourceWrapper dataSource) {
        try (final ConnectionWrapper connection = dataSource.getConnection(true)) {
            final DatabaseMetaData metaData = connection.get()
                                                        .getMetaData();
            for (PredefinedMapping mapping : values()) {
                final boolean nameMatch = metaData.getDatabaseProductName()
                                                  .equals(mapping.databaseProductName);
                final boolean versionMatch =
                        metaData.getDatabaseMajorVersion() == mapping.majorVersion
                        && metaData.getDatabaseMinorVersion() == mapping.minorVersion;
                if (nameMatch && versionMatch) {
                    return mapping;
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return MYSQL_5_7;
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
