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
import io.spine.server.storage.jdbc.TypeMapping.Builder;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;

/**
 * Standard {@link TypeMapping type mappings} for different databases.
 *
 * @author Dmytro Grankin
 */
public final class TypeMappings {

    private static final TypeMapping MYSQL_5 = baseMappingBuilder().add(BYTE_ARRAY, "BLOB")
                                                                   .build();
    private static final TypeMapping POSTGRESQL_10 = baseMappingBuilder().add(BYTE_ARRAY, "BYTEA")
                                                                         .build();

    private TypeMappings() {
        // Prevent instantiation of this utility class.
    }

    /**
     * Obtains the type mapping for MySQL 5 database.
     */
    @VisibleForTesting
    public static TypeMapping mySql() {
        return MYSQL_5;
    }

    /**
     * Obtains the type mapping for PostreSQL 10 database.
     */
    @VisibleForTesting
    public static TypeMapping postgreSql() {
        return POSTGRESQL_10;
    }

    /**
     * Obtains the type mapping for the used
     * {@linkplain DatabaseMetaData#getDatabaseProductName() database}.
     *
     * <p>The mappings may differ for various versions of the same database,
     * so {@linkplain DatabaseMetaData#getDatabaseMajorVersion() major database version}
     * either affects the selection of the mapping.
     * 
     * @param dataSource the data source to get database metadata
     * @return the type mapping for the used database
     *         or MySQL-specific mapping if there is no standard mapping for the database
     */
    static TypeMapping get(DataSourceWrapper dataSource) {
        try (final ConnectionWrapper connection = dataSource.getConnection(true)) {
            final DatabaseMetaData metaData = connection.get()
                                                        .getMetaData();
            final String databaseName = metaData.getDatabaseProductName()
                                                .toLowerCase();
            final int majorVersion = metaData.getDatabaseMajorVersion();
            if ("postgresql".equals(databaseName) && majorVersion == 10) {
                return POSTGRESQL_10;
            }

            return MYSQL_5;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static TypeMapping.Builder baseMappingBuilder() {
        final Builder baseMapping = TypeMapping.newBuilder()
                                               .add(INT, "INT")
                                               .add(LONG, "BIGINT")
                                               .add(STRING_255, "VARCHAR(255)")
                                               .add(STRING, "TEXT")
                                               .add(BOOLEAN, "BOOLEAN");
        return baseMapping;
    }
}
