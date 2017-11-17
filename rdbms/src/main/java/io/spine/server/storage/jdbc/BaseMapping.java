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
import java.util.EnumMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * A base implementation of {@link TypeMapping}.
 *
 * @author Dmytro Grankin
 */
public final class BaseMapping implements TypeMapping {

    private final Map<Type, String> mappedTypes;
    private final DatabaseProductName databaseProductName;
    private final int majorVersion;

    private BaseMapping(Builder builder) {
        this.mappedTypes = builder.mappedTypes;
        this.databaseProductName = builder.databaseProductName;
        this.majorVersion = builder.majorVersion;
    }

    @Override
    public String getTypeName(Type type) {
        checkState(mappedTypes.containsKey(type),
                   "The type mapping doesn't define name for %s type.", type);
        final String name = mappedTypes.get(type);
        return name;
    }

    /**
     * Determines whether the mapping is suitable to work with the data source.
     *
     * @param dataSource the data source to test compatibility
     * @return {@code true} if the {@linkplain DatabaseMetaData#getDatabaseProductName()
     *         database product name} and the {@linkplain DatabaseMetaData#getDatabaseMajorVersion()
     *         major version} are equal to the used in the mapping
     */
    public boolean suitableFor(DataSourceWrapper dataSource) {
        try (final ConnectionWrapper connection = dataSource.getConnection(true)) {
            final DatabaseMetaData metaData = connection.get()
                                                        .getMetaData();
            final DatabaseProductName currentDbProductName =
                    DatabaseProductName.valueOf(metaData.getDatabaseProductName());
            final boolean nameMatch = currentDbProductName == databaseProductName;
            final boolean versionMatch = metaData.getDatabaseMajorVersion() == majorVersion;
            return nameMatch && versionMatch;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @VisibleForTesting
    DatabaseProductName getDatabaseProductName() {
        return databaseProductName;
    }

    /**
     * Creates a new instance of the {@link BaseMapping} builder.
     *
     * <p>All the {@link Type types} should be mapped.
     *
     * @return the new builder instance
     */
    static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for {@link BaseMapping}.
     */
    public static class Builder {

        private final Map<Type, String> mappedTypes = new EnumMap<>(Type.class);
        private DatabaseProductName databaseProductName;
        private int majorVersion;

        private Builder() {
            // Prevent direct instantiation of this class.
        }

        /**
         * Adds a mapping for the specified type.
         *
         * <p>Overrides the name of the type if it is already specified.
         *
         * @param type the type for the mapping
         * @param name the custom name for the type
         * @return the builder instance
         */
        public Builder add(Type type, String name) {
            checkNotNull(type);
            checkArgument(!isNullOrEmpty(name));
            mappedTypes.put(type, name);
            return this;
        }

        /**
         * Sets the target {@link DatabaseProductName} for the mapping.
         */
        Builder setDatabaseName(DatabaseProductName databaseName) {
            this.databaseProductName = checkNotNull(databaseName);
            return this;
        }

        /**
         * Sets the target {@linkplain DatabaseMetaData#getDatabaseMajorVersion()
         * database major version} for the mapping.
         */
        Builder setMajorVersion(int majorVersion) {
            this.majorVersion = majorVersion;
            return this;
        }

        /**
         * Creates {@link BaseMapping} for the builder.
         *
         * @return a new type mapping
         * @throws IllegalStateException if not all of the {@linkplain Type types} were mapped
         */
        public BaseMapping build() {
            final int typesCount = Type.values().length;
            checkState(mappedTypes.size() == typesCount,
                       "A mapping should contain names for all types (%s), " +
                       "but only (%s) types were mapped.", typesCount, mappedTypes.size());
            return new BaseMapping(this);
        }
    }

    /**
     * Names of {@linkplain DatabaseMetaData#getDatabaseProductName() database products}.
     */
    public enum DatabaseProductName {

        PostgreSQL,
        MySQL
    }
}
