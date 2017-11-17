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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.BaseMapping.baseBuilder;
import static io.spine.server.storage.jdbc.StandardMappings.DatabaseProductName.MySQL;
import static io.spine.server.storage.jdbc.StandardMappings.DatabaseProductName.PostgreSQL;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;

/**
 * Standard {@linkplain TypeMapping type mappings} for different databases.
 *
 * @author Dmytro Grankin
 */
public final class StandardMappings {

    private static final SelectableMapping MYSQL_5 = new SelectableMapping(MySQL, 5,
                                                                           baseBuilder().build());
    private static final SelectableMapping POSTGRESQL_10 =
            new SelectableMapping(PostgreSQL, 10,
                                  baseBuilder().add(BYTE_ARRAY, "BYTEA")
                                               .build());

    private StandardMappings() {
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
     * Obtains the {@linkplain SelectableMapping#suitableFor(DataSourceWrapper) suitable}
     * type mapping for the specified data source.
     *
     * @param dataSource the data source to test suitability
     * @return the type mapping for the used database
     *         or MySQL-specific mapping if there is no standard mapping for the database
     */
    static TypeMapping get(DataSourceWrapper dataSource) {
        if (POSTGRESQL_10.suitableFor(dataSource)) {
            return POSTGRESQL_10;
        }

        return MYSQL_5;
    }

    /**
     * Names of {@linkplain DatabaseMetaData#getDatabaseProductName() database products}.
     */
    enum DatabaseProductName {

        PostgreSQL,
        MySQL
    }

    /**
     * A mapping for a {@linkplain #suitableFor(DataSourceWrapper) specific} data source.
     */
    static class SelectableMapping implements TypeMapping {

        private final TypeMapping mapping;
        private final DatabaseProductName databaseProductName;
        private final int majorVersion;

        SelectableMapping(DatabaseProductName databaseProductName, int majorVersion,
                          TypeMapping mapping) {
            this.mapping = checkNotNull(mapping);
            this.databaseProductName = checkNotNull(databaseProductName);
            this.majorVersion = majorVersion;
        }

        @Override
        public String getTypeName(Type type) {
            return  mapping.getTypeName(type);
        }

        /**
         * Determines whether the mapping is suitable to work with the data source.
         *
         * @param dataSource the data source to test compatibility
         * @return {@code true} if the {@linkplain DatabaseMetaData#getDatabaseProductName()
         *         database product name} and the {@linkplain DatabaseMetaData#getDatabaseMajorVersion()
         *         major version} are equal to the used in the mapping
         */
        @VisibleForTesting
        boolean suitableFor(DataSourceWrapper dataSource) {
            try (final ConnectionWrapper connection = dataSource.getConnection(true)) {
                final DatabaseMetaData metaData = connection.get()
                                                            .getMetaData();
                final boolean nameMatch = databaseProductName.name()
                                                             .equals(metaData.getDatabaseProductName());
                final boolean versionMatch = majorVersion == metaData.getDatabaseMajorVersion();
                return nameMatch && versionMatch;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }
}
