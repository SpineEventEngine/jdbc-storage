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

import java.sql.SQLException;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;

/**
 * Default {@link TypeMapping type mappings} for different databases.
 *
 * @author Dmytro Grankin
 */
public class TypeMappings {

    private static final TypeMapping MY_SQL = baseMapping().add(BYTE_ARRAY, "BLOB")
                                                           .build();
    private static final TypeMapping POSTGRES = baseMapping().add(BYTE_ARRAY, "BYTEA")
                                                             .build();

    private TypeMappings() {
        // Prevent instantiation of this utility class.
    }

    /**
     * Obtains the default type mapping for MySQL database.
     */
    @VisibleForTesting
    public static TypeMapping mySql() {
        return MY_SQL;
    }

    /**
     * Obtains the default type mapping for PostreSQL database.
     */
    @VisibleForTesting
    public static TypeMapping postgreSql() {
        return POSTGRES;
    }

    /**
     * Obtains the type mapping for the used
     * {@linkplain java.sql.DatabaseMetaData#getDatabaseProductName() database}.
     * 
     * @param dataSource the data source to get database metadata
     * @return the type mapping for the used database
     *         or {@linkplain #mySql() MySQL} mapping if there is no standard mapping for the database
     */
    static TypeMapping get(DataSourceWrapper dataSource) {
        try (final ConnectionWrapper connection = dataSource.getConnection(true)) {
            final String name = connection.get()
                                          .getMetaData()
                                          .getDatabaseProductName()
                                          .toLowerCase();
            if ("postgresql".equals(name)) {
                return POSTGRES;
            }

            return MY_SQL;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static TypeMapping.Builder baseMapping() {
        final Builder baseMapping = TypeMapping.newBuilder()
                                               .add(INT, "INT")
                                               .add(LONG, "BIGINT")
                                               .add(STRING_255, "VARCHAR(255)")
                                               .add(STRING, "TEXT")
                                               .add(BOOLEAN, "BOOLEAN");
        return baseMapping;
    }
}
