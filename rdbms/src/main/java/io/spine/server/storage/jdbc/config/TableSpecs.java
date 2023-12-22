/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.config;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
import io.spine.server.storage.jdbc.record.TableNames;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Preconditions2.checkNotEmptyOrBlank;
import static java.util.Objects.requireNonNull;

/**
 * The set of custom database table settings as configured by the library users,
 * per type of the stored record.
 */
@Internal
public final class TableSpecs {

    private final ImmutableMap<Class<? extends Message>, String> names;
    private final Map<Class<? extends Message>, JdbcTableSpec<?, ?>> tables = new HashMap<>();

    private final ImmutableMap<Class<? extends Message>, JdbcColumnMapping> columnMappings;

    /**
     * Creates the settings instance on top of the passed builder.
     */
    private TableSpecs(Builder builder) {
        this.names = ImmutableMap.copyOf(builder.names);
        this.columnMappings = ImmutableMap.copyOf(builder.mappings);
    }

    /**
     * Provides the table specification based upon the original record specification,
     * and the user-defined configuration previously made with
     * this instance of {@code TableSpecs}, such as table name and custom column mapping.
     *
     * <p>In case no custom table name was specified,
     * a {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class)
     * default one} is used.
     *
     * <p>If no custom column mapping was set previously,
     * the default mapping passed to this method is used.
     *
     * @param spec
     *         the original record specification
     * @param defaultMapping
     *         the column mapping to use if no custom mapping is specified for the table
     * @param <I>
     *         type of the identifiers of the records to store in the table
     * @param <R>
     *         type of the records stored in the table
     * @return a new table specification
     */
    public <I, R extends Message> JdbcTableSpec<I, R>
    specFor(RecordSpec<I, R> spec, JdbcColumnMapping defaultMapping) {
        var recordType = spec.sourceType();
        if (!tables.containsKey(recordType)) {
            var tableSpec = newTableSpec(spec, defaultMapping);
            tables.put(recordType, tableSpec);
        }
        @SuppressWarnings("unchecked")
        var result = (JdbcTableSpec<I, R>) tables.get(recordType);
        return requireNonNull(result);
    }

    private <I, R extends Message> JdbcTableSpec<I, R>
    newTableSpec(RecordSpec<I, R> spec, JdbcColumnMapping defaultMapping) {
        var recordType = spec.recordType();
        @Nullable String customName = findName(recordType);
        @Nullable JdbcColumnMapping customMapping = findMapping(recordType);

        JdbcTableSpec<I, R> tableSpec;
        var tableName = customName == null
                        ? TableNames.of(spec.sourceType())
                        : customName;

        var mapping = customMapping == null
                      ? defaultMapping
                      : customMapping;

        tableSpec = new JdbcTableSpec<>(tableName, spec, mapping);
        return tableSpec;
    }

    private <R extends Message> @Nullable String findName(Class<R> recordType) {
        @Nullable String customName = null;
        if (names.containsKey(recordType)) {
            customName = names.get(recordType);
        }
        return customName;
    }

    private <R extends Message> @Nullable JdbcColumnMapping findMapping(Class<R> recordType) {
        @Nullable JdbcColumnMapping value = null;
        if (columnMappings.containsKey(recordType)) {
            value = columnMappings.get(recordType);
        }
        return value;
    }

    /**
     * Creates a new {@code Builder} for this type.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder of the {@code TableSpecs} instances.
     */
    public static final class Builder {

        private final Map<Class<? extends Message>, String> names = new HashMap<>();

        private final Map<Class<? extends Message>, JdbcColumnMapping> mappings = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the custom DB table name for the table storing the records of the specified type.
         *
         * <p>The name previously set, if any, is replaced with this call.
         *
         * <p>The name cannot be blank.
         *
         * <p>In case no custom name is defined,
         * a {@linkplain  io.spine.server.storage.jdbc.record.TableNames#of(Class) default name}
         * is used.
         *
         * @param recordType
         *         the type of the stored record
         * @param name
         *         the table name
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message>
        Builder setTableName(Class<R> recordType, String name) {
            checkNotNull(recordType);
            checkNotEmptyOrBlank(name);
            this.names.put(recordType, name);
            return this;
        }

        /**
         * Sets the column type mapping rules for the table, in which the records of the specified
         * type are stored.
         *
         * <p>This mapping ruleset will override
         * the {@linkplain io.spine.server.storage.jdbc.JdbcStorageFactory#columnMapping()
         * factory-wide} setting for this particular table.
         *
         * <p>Previously set mapping value, if any, is replaced with this call.
         *
         * @param recordType
         *         the type of the stored record
         * @param mapping
         *         the custom set of type mapping rules
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message>
        Builder setMapping(Class<R> recordType, JdbcColumnMapping mapping) {
            checkNotNull(recordType);
            checkNotNull(mapping);
            this.mappings.put(recordType, mapping);
            return this;
        }

        /**
         * Creates a new {@code TableSpecs} instance.
         */
        public TableSpecs build() {
            return new TableSpecs(this);
        }
    }
}
