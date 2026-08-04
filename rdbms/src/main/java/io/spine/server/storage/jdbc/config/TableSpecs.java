/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.spine.server.storage.StorageGroup;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
import io.spine.server.storage.jdbc.record.TableNames;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Preconditions2.checkNotEmptyOrBlank;

/**
 * The set of custom database table settings as configured by the library users,
 * per type of the stored record.
 *
 * <p>Also serves as a cache of the table specifications created for the record
 * specifications passed to {@link #specFor(RecordSpec, StorageGroup, JdbcColumnMapping)
 * specFor(..)}. Each table is identified by the combination of the source type
 * and the record type of the record specification, along with the name of
 * the {@link StorageGroup}, if any.
 */
@Internal
public final class TableSpecs {

    private final ImmutableMap<Class<? extends Message>, String> names;
    private final Map<SpecKey, JdbcTableSpec<?, ?>> tables = new ConcurrentHashMap<>();

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
     * the storage group, and the user-defined configuration previously made with
     * this instance of {@code TableSpecs}, such as table name and custom column mapping.
     *
     * <p>The specifications are cached, so that equal combinations of the source type,
     * the record type, and the group always resolve to the same table. This method
     * tolerates concurrent invocations, as some storages are created lazily
     * on worker threads.
     *
     * <p>For the storages belonging to no group, in case no custom table name
     * was specified, a {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class)
     * default one} is used.
     *
     * <p>The tables of grouped storages are always named after
     * the {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class, StorageGroup)
     * group and the record type}. Custom table names do not apply to them: such names
     * are registered per record type, which cannot tell apart the storages
     * of one group from another — e.g., the event journals of all entity types
     * store records of the same {@code Event} type.
     *
     * <p>If no custom column mapping was set previously,
     * the default mapping passed to this method is used.
     *
     * @param spec
     *         the original record specification
     * @param group
     *         the group to which the storage belongs, or {@code null} if it belongs to none
     * @param defaultMapping
     *         the column mapping to use if no custom mapping is specified for the table
     * @param <I>
     *         type of the identifiers of the records to store in the table
     * @param <R>
     *         type of the records stored in the table
     * @return the table specification
     */
    public <I, R extends Message> JdbcTableSpec<I, R>
    specFor(RecordSpec<I, R> spec, @Nullable StorageGroup group, JdbcColumnMapping defaultMapping) {
        var key = SpecKey.of(spec, group);
        var tableSpec = tables.computeIfAbsent(key, k -> newTableSpec(spec, group, defaultMapping));
        @SuppressWarnings("unchecked")
        var result = (JdbcTableSpec<I, R>) tableSpec;
        return result;
    }

    private <I, R extends Message> JdbcTableSpec<I, R>
    newTableSpec(RecordSpec<I, R> spec,
                 @Nullable StorageGroup group,
                 JdbcColumnMapping defaultMapping) {
        @Nullable JdbcColumnMapping customMapping = findMapping(spec.recordType());
        var mapping = customMapping == null
                      ? defaultMapping
                      : customMapping;
        var tableName = tableName(spec, group);
        return new JdbcTableSpec<>(tableName, spec, mapping);
    }

    private String tableName(RecordSpec<?, ?> spec, @Nullable StorageGroup group) {
        if (group != null) {
            return TableNames.of(spec.recordType(), group);
        }
        @Nullable String customName = findName(spec.recordType());
        return customName == null
               ? TableNames.of(spec.sourceType())
               : customName;
    }

    /**
     * The identity of a table: the source and the record types of the stored records,
     * and the name of the storage group, if any.
     *
     * @param sourceType
     *         the source type of the record specification
     * @param recordType
     *         the type of the stored records
     * @param group
     *         the name of the storage group,
     *         or {@code null} if the storage belongs to no group
     */
    private record SpecKey(Class<? extends Message> sourceType,
                           Class<? extends Message> recordType,
                           @Nullable String group) {

        private static SpecKey of(RecordSpec<?, ?> spec, @Nullable StorageGroup group) {
            var groupName = group == null ? null : group.getName();
            return new SpecKey(spec.sourceType(), spec.recordType(), groupName);
        }
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
