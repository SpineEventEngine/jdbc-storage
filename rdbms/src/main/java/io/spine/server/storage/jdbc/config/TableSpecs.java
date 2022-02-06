/*
 * Copyright 2022, TeamDev. All rights reserved.
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
import io.spine.server.storage.jdbc.record.column.CustomColumns;
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

    /**
     * Values of settings per the type of the records served by the configured storage.
     */
    private final ImmutableMap<Class<? extends Message>, CustomColumns<?>> columns;
    private final ImmutableMap<Class<? extends Message>, String> names;
    private final Map<Class<? extends Message>, JdbcTableSpec<?, ?>> tables = new HashMap<>();

    /**
     * Creates the settings instance on top of the passed builder.
     */
    private TableSpecs(Builder builder) {
        this.columns = ImmutableMap.copyOf(builder.columns);
        this.names = ImmutableMap.copyOf(builder.names);
    }

    /**
     * Provides the table specification based upon the original record specification,
     * JDBC column mapping, and the user-defined configuration previously made with
     * this instance of {@code TableSpecs}.
     *
     * @param spec
     *         the original record specification
     * @param mapping
     *         the column mapping
     * @param <I>
     *         type of the identifiers of the records to store in the table
     * @param <R>
     *         type of the records stored in the table
     * @return a new table specification
     */
    public <I, R extends Message> JdbcTableSpec<I, R>
    specFor(RecordSpec<I, R, ?> spec, JdbcColumnMapping mapping) {
        var recordType = spec.sourceType();
        if (!tables.containsKey(recordType)) {
            var tableSpec = newTableSpec(spec, mapping);
            tables.put(recordType, tableSpec);
        }
        @SuppressWarnings("unchecked")
        var result = (JdbcTableSpec<I, R>) tables.get(recordType);
        return requireNonNull(result);
    }

    private <I, R extends Message> JdbcTableSpec<I, R>
    newTableSpec(RecordSpec<I, R, ?> spec, JdbcColumnMapping mapping) {
        var recordType = spec.storedType();
        @Nullable CustomColumns<R> customCols = findColumns(recordType);
        @Nullable String customName = findName(recordType);

        JdbcTableSpec<I, R> tableSpec;
        if (customName != null) {
            tableSpec = new JdbcTableSpec<>(customName, spec, mapping, customCols);
        } else {
            tableSpec = new JdbcTableSpec<>(spec, mapping, customCols);
        }
        return tableSpec;
    }

    @Nullable
    private <R extends Message> String findName(Class<R> recordType) {
        @Nullable String customName = null;
        if(names.containsKey(recordType)) {
            customName = names.get(recordType);
        }
        return customName;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private <R extends Message> CustomColumns<R> findColumns(Class<R> recordType) {
        @Nullable CustomColumns<R> customCols = null;
        if(columns.containsKey(recordType)) {
            var raw  = columns.get(recordType);
            customCols = (CustomColumns<R>) raw;
        }
        return customCols;
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

        private final Map<Class<? extends Message>, CustomColumns<?>> columns = new HashMap<>();
        private final Map<Class<? extends Message>, String> names = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the custom columns for the table storing the records of the specified type.
         *
         * <p>Previously set values, if any, are replaced with this call.
         *
         * @param recordType
         *         the type of the stored record
         * @param columns
         *         the custom columns
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message>
        Builder setColumns(Class<R> recordType, CustomColumns<R> columns) {
            checkNotNull(recordType);
            checkNotNull(columns);
            this.columns.put(recordType, columns);
            return this;
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
         * Creates a new {@code TableSpecs} instance.
         */
        public TableSpecs build() {
            return new TableSpecs(this);
        }
    }
}
