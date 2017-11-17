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

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.server.storage.jdbc.TypeMapping.DatabaseProductName.mysql;
import static io.spine.server.storage.jdbc.TypeMapping.DatabaseProductName.postresql;

/**
 * Standard {@link TypeMapping type mappings} for different databases.
 *
 * @author Dmytro Grankin
 */
public final class TypeMappings {

    private static final TypeMapping MYSQL_5 = baseBuilder().setDatabaseName(mysql)
                                                            .setMajorVersion(5)
                                                            .build();
    private static final TypeMapping POSTGRESQL_10 = baseBuilder().add(BYTE_ARRAY, "BYTEA")
                                                                  .setDatabaseName(postresql)
                                                                  .setMajorVersion(10)
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
     * Obtains the {@linkplain TypeMapping#suitableFor(DataSourceWrapper) suitable} type mapping
     * for the specified data source.
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
     * Obtains the base builder for mappings.
     *
     * <p>All the types are mapped as follows:
     * <ul>
     *     <li>{@code Type.BYTE_ARRAY} - BLOB</li>
     *     <li>{@code Type.INT} - INT</li>
     *     <li>{@code Type.LONG} - BIGINT</li>
     *     <li>{@code Type.STRING_255} - VARCHAR(255)</li>
     *     <li>{@code Type.STRING} - TEXT</li>
     *     <li>{@code Type.BOOLEAN} - BOOLEAN</li>
     * </ul>
     *
     * <p>{@linkplain Builder#add(Type, String) Override} the type name
     * if it doesn't match a database.
     *
     * @return the builder with all types
     */
    public static TypeMapping.Builder baseBuilder() {
        final Builder baseMapping = TypeMapping.newBuilder()
                                               .add(BYTE_ARRAY, "BLOB")
                                               .add(INT, "INT")
                                               .add(LONG, "BIGINT")
                                               .add(STRING_255, "VARCHAR(255)")
                                               .add(STRING, "TEXT")
                                               .add(BOOLEAN, "BOOLEAN");
        return baseMapping;
    }
}
