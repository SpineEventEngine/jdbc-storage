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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.BaseMapping.baseBuilder;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;

/**
 * Standard {@linkplain TypeMapping type mappings} for different databases.
 *
 * @author Dmytro Grankin
 */
public enum StandardMapping implements TypeMapping {

    MYSQL_5("MySQL", 5, baseBuilder().build()),
    POSTRESQL_10("PostreSQL", 10, baseBuilder().add(BYTE_ARRAY, "BYTEA")
                                               .build());

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final TypeMapping typeMapping;
    private final String databaseProductName;
    private final int majorVersion;

    StandardMapping(String databaseProductName, int majorVersion, TypeMapping typeMapping) {
        this.databaseProductName = databaseProductName;
        this.majorVersion = majorVersion;
        this.typeMapping = typeMapping;
    }

    @Override
    public String getTypeName(Type type) {
        return typeMapping.getTypeName(type);
    }

    /**
     * Selects the type mapping for the specified data source.
     *
     * <p>The {@linkplain DatabaseMetaData#getDatabaseProductName() database product name} and
     * the {@linkplain DatabaseMetaData#getDatabaseMajorVersion() major version} are taken into
     * account during the selection.
     *
     * @param dataSource the data source to test suitability
     * @return the type mapping for the used database
     *         or MySQL-specific mapping if there is no standard mapping for the database
     */
    static TypeMapping select(DataSourceWrapper dataSource) {
        try (final ConnectionWrapper connection = dataSource.getConnection(true)) {
            final DatabaseMetaData metaData = connection.get()
                                                        .getMetaData();
            for (StandardMapping mapping : values()) {
                final boolean nameMatch = metaData.getDatabaseProductName()
                                                  .equals(mapping.databaseProductName);
                final boolean versionMatch =
                        metaData.getDatabaseMajorVersion() == mapping.majorVersion;
                if (nameMatch && versionMatch) {
                    return mapping;
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return MYSQL_5;
    }

    @VisibleForTesting
    String getDatabaseProductName() {
        return databaseProductName;
    }

    @VisibleForTesting
    int getMajorVersion() {
        return majorVersion;
    }
}
