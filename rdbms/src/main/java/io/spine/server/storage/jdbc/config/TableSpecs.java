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
import io.spine.base.EntityState;
import io.spine.core.BoundedContextName;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.StorageGroup;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
import io.spine.server.storage.jdbc.record.TableNames;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import io.spine.type.TypeName;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Preconditions2.checkNotEmptyOrBlank;
import static java.lang.String.format;

/**
 * The set of custom database table settings as configured by the library users,
 * per type of the stored record.
 *
 * <p>Also serves as a cache of the table specifications created for the record
 * specifications passed to {@link #specFor(RecordSpec, StorageGroup, JdbcColumnMapping)
 * specFor(..)}. Each table is identified by the combination of the source type
 * and the record type of the record specification, along with the name of
 * the {@link StorageGroup}, if any.
 *
 * <p>Composing a table name is not injective: distinct names — e.g. those of
 * the Bounded Contexts {@code Sales.EU} and {@code Sales_EU} — may resolve to one
 * table name once the {@linkplain TableNames prohibited characters are replaced}.
 * This instance tracks the names claimed by the created specifications, and creating
 * a specification whose table name is already claimed by a different storage fails
 * with an {@link IllegalStateException} naming both claimants. The names are compared
 * truncated to {@value #MAX_IDENTIFIER_BYTES} bytes — the identifier limit which
 * PostgreSQL applies silently — so the names differing only past that limit are
 * treated as clashing, too.
 */
@Internal
public final class TableSpecs {

    /**
     * The strictest identifier length limit among the supported database engines,
     * in bytes: PostgreSQL truncates longer identifiers silently.
     */
    private static final int MAX_IDENTIFIER_BYTES = 63;

    /**
     * The custom table names registered for the storages belonging to no group,
     * per the source type of their record specifications.
     */
    private final ImmutableMap<Class<? extends Message>, String> names;

    /**
     * The custom table names registered for the grouped storages,
     * per the identity of a grouped table.
     */
    private final ImmutableMap<GroupedTable, String> groupedNames;

    /**
     * The cache of the created table specifications, per the identity of a table.
     */
    private final Map<SpecKey, JdbcTableSpec<?, ?>> tables = new ConcurrentHashMap<>();

    /**
     * The effective table names claimed by the created specifications,
     * to the keys of their claimants.
     */
    private final Map<String, SpecKey> claimedNames = new ConcurrentHashMap<>();

    /**
     * The custom column mappings overriding the factory-wide default,
     * per the source type of the record specifications.
     */
    private final ImmutableMap<Class<? extends Message>, JdbcColumnMapping> columnMappings;

    /**
     * Creates the settings instance on top of the passed builder.
     */
    private TableSpecs(Builder builder) {
        this.names = ImmutableMap.copyOf(builder.names);
        this.groupedNames = ImmutableMap.copyOf(builder.groupedNames);
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
     * <p>Custom table names and custom column mappings are looked up by
     * the source type of the record specification — for an entity storage,
     * the entity state type; for a standalone record, the record type itself.
     *
     * <p>For the storages belonging to no group, in case no custom table name
     * was specified, a {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class)
     * default one} is used.
     *
     * <p>The tables of grouped storages take the custom names registered with
     * {@link Builder#setTableName(Class, Class, String)}, addressed by the group
     * and the record type. The single-type custom names do not apply to them:
     * the event journals of all entity types share one source type, {@code Event},
     * and the state history of an entity shares its source type with
     * the latest-state storage, so such a name would collide the tables.
     * In case no custom name is registered for a grouped table, it is named after
     * the {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class, StorageGroup)
     * group and the record type}.
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
        var tableSpec = tables.computeIfAbsent(
                key, k -> newTableSpec(k, spec, group, defaultMapping));
        @SuppressWarnings("unchecked")
        var result = (JdbcTableSpec<I, R>) tableSpec;
        return result;
    }

    private <I, R extends Message> JdbcTableSpec<I, R>
    newTableSpec(SpecKey key,
                 RecordSpec<I, R> spec,
                 @Nullable StorageGroup group,
                 JdbcColumnMapping defaultMapping) {
        var customMapping = findMapping(spec.sourceType());
        var mapping = customMapping == null
                      ? defaultMapping
                      : customMapping;
        var tableName = tableName(spec, group);
        claim(tableName, key);
        return new JdbcTableSpec<>(tableName, spec, mapping);
    }

    /**
     * Claims the effective table name for the storage with the given key,
     * ensuring no other storage has claimed it before.
     *
     * @throws IllegalStateException
     *         if the name is already claimed by a storage with a different key
     */
    private void claim(String tableName, SpecKey key) {
        var effective = effectiveName(tableName);
        var previous = claimedNames.putIfAbsent(effective, key);
        if (previous != null && !previous.equals(key)) {
            throw new IllegalStateException(format(
                    "The storages identified by `%s` and `%s` " +
                            "would share the DB table `%s`. " +
                            "Rename one of the storage groups — " +
                            "such as the Bounded Context — or assign " +
                            "a distinct table name via `setTableName(..)`.",
                    previous, key, effective));
        }
    }

    /**
     * Returns the name as seen by the strictest supported database engine:
     * truncated to {@value #MAX_IDENTIFIER_BYTES} bytes, if longer.
     */
    private static String effectiveName(String name) {
        var bytes = name.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_IDENTIFIER_BYTES) {
            return name;
        }
        return new String(bytes, 0, MAX_IDENTIFIER_BYTES, StandardCharsets.UTF_8);
    }

    private String tableName(RecordSpec<?, ?> spec, @Nullable StorageGroup group) {
        if (group != null) {
            var customName = groupedNames.get(new GroupedTable(group.getName(), spec.recordType()));
            return customName == null
                   ? TableNames.of(spec.recordType(), group)
                   : customName;
        }
        var customName = findName(spec.sourceType());
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

    /**
     * The identity of a grouped table, as addressed by the custom-name registration:
     * the name of the storage group, and the type of the stored records.
     *
     * @param group
     *         the name of the storage group
     * @param recordType
     *         the type of the stored records
     */
    private record GroupedTable(String group, Class<? extends Message> recordType) {

        private static GroupedTable of(Class<? extends EntityState<?>> stateType,
                                       Class<? extends Message> recordType) {
            var groupName = TypeName.of(stateType).value();
            return new GroupedTable(groupName, recordType);
        }
    }

    private @Nullable String findName(Class<? extends Message> sourceType) {
        String customName = null;
        if (names.containsKey(sourceType)) {
            customName = names.get(sourceType);
        }
        return customName;
    }

    private @Nullable JdbcColumnMapping findMapping(Class<? extends Message> sourceType) {
        JdbcColumnMapping value = null;
        if (columnMappings.containsKey(sourceType)) {
            value = columnMappings.get(sourceType);
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

        private final Map<GroupedTable, String> groupedNames = new HashMap<>();

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
         * Sets the custom DB table name for the grouped table which serves the entities
         * with the specified state type, storing the records of the specified type.
         *
         * <p>The grouped table is addressed by the storage group — named by the framework
         * after the entity state type — paired with the type of the stored records.
         *
         * <p>The name previously set for the same grouped table, if any,
         * is replaced with this call.
         *
         * <p>The name cannot be blank.
         *
         * @param stateType
         *         the type of the state of the entity served by the grouped storage
         * @param recordType
         *         the type of the records stored by the grouped storage
         * @param name
         *         the table name
         * @param <S>
         *         the type of the entity state
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <S extends EntityState<?>, R extends Message>
        Builder setTableName(Class<S> stateType, Class<R> recordType, String name) {
            checkNotNull(stateType);
            checkNotNull(recordType);
            checkNotEmptyOrBlank(name);
            this.groupedNames.put(GroupedTable.of(stateType, recordType), name);
            return this;
        }

        /**
         * Sets the custom DB table name for the grouped table which serves
         * the Bounded Context with the given name, storing the records of
         * the specified type — such as the event store of the context.
         *
         * <p>The grouped table is addressed by the storage group — named by
         * the framework after the context, taking its name verbatim (see
         * {@link StorageGroup#of(BoundedContextName)}) — paired with the type
         * of the stored records. To address the table of a System context,
         * spell its name directly, e.g.
         * {@code BoundedContextNames.newName("Billing_System")}.
         *
         * <p>The name previously set for the same grouped table, if any,
         * is replaced with this call.
         *
         * <p>The name cannot be blank.
         *
         * @param context
         *         the name of the Bounded Context served by the grouped storage
         * @param recordType
         *         the type of the records stored by the grouped storage
         * @param name
         *         the table name
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message>
        Builder setTableName(BoundedContextName context, Class<R> recordType, String name) {
            checkNotNull(context);
            checkNotNull(recordType);
            checkNotEmptyOrBlank(name);
            var group = StorageGroup.of(context);
            this.groupedNames.put(new GroupedTable(group.getName(), recordType), name);
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
