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

package io.spine.server.storage.jdbc.record;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.protobuf.Messages;
import io.spine.query.Column;
import io.spine.query.ColumnName;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.record.column.BytesColumn;
import io.spine.server.storage.jdbc.record.column.IdColumn;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.spine.util.Preconditions2.checkNotEmptyOrBlank;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The specification of the JDBC-accessible database table
 * telling how the record values should be stored.
 *
 * @param <I>
 *         the type of identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
public final class JdbcTableSpec<I, R extends Message> {

    private final RecordSpec<I, R> recordSpec;
    private final String tableName;
    private final JdbcColumnMapping columnMapping;
    private final IdColumn<I> idColumn;
    private final Descriptor recordDescriptor;
    private final ImmutableMap<ColumnName, TableColumn> dataColumns;

    /**
     * Creates a new table specification, also setting a custom name for the table.
     *
     * <p>It is a responsibility of callers to select the table name which is both unique and
     * compatible with the requirements of the underlying database engine.
     *
     * @param tableName
     *         the name to use for the table
     * @param recordSpec
     *         the original specification of the stored record
     * @param mapping
     *         the column mapping to use
     */
    public JdbcTableSpec(String tableName, RecordSpec<I, R> recordSpec, JdbcColumnMapping mapping) {
        this.tableName = checkNotEmptyOrBlank(tableName);
        this.recordSpec = recordSpec;
        columnMapping = mapping;
        this.idColumn = IdColumn.of(recordSpec, columnMapping);
        this.recordDescriptor = descriptorFrom(recordSpec.recordType());
        this.dataColumns = createDataColumns();
    }

    private static Descriptor descriptorFrom(Class<? extends Message> type) {
        return Messages.getDefaultInstance(type)
                       .getDescriptorForType();
    }

    /**
     * Returns the name of the described table.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * Returns the column of this table which stores the record identifiers.
     */
    public IdColumn<I> idColumn() {
        return idColumn;
    }

    /**
     * Returns the identifier of the record.
     */
    public I idFromRecord(R record) {
        return recordSpec.idValueIn(record);
    }

    /**
     * Returns the Proto descriptor of the type, records of which are stored in the table.
     */
    public Descriptor recordDescriptor() {
        return recordDescriptor;
    }

    /**
     * Returns the column mapping used by the table.
     */
    public JdbcColumnMapping columnMapping() {
        return columnMapping;
    }

    /**
     * Returns all table columns except for the {@linkplain #idColumn() ID column}.
     */
    public ImmutableCollection<TableColumn> dataColumns() {
        return dataColumns.values();
    }

    /**
     * Returns the names of the {@linkplain #dataColumns() data columns}.
     */
    ImmutableSet<ColumnName> columnNames() {
        return dataColumns.keySet();
    }

    private ImmutableMap<ColumnName, TableColumn> createDataColumns() {
        var cols = new LinkedHashMap<ColumnName, TableColumn>();
        addBytesColumn(cols);
        for (var column : recordSpec.columns()) {
            var nativeColumn = toNativeColumn(column);
            cols.put(column.name(), nativeColumn);
        }
        var result = ImmutableMap.copyOf(cols);
        return result;
    }

    private TableColumn toNativeColumn(Column<?, ?> column) {
        var name = column.name();
        var nativeColumn = new TableColumn(name.value(), column.type(), columnMapping);
        return nativeColumn;
    }

    private void addBytesColumn(Map<ColumnName, TableColumn> cols) {
        var bytesColumnName = ColumnName.of(BytesColumn.bytesColumnName());
        var bytesColumn = new BytesColumn(columnMapping);
        cols.put(bytesColumnName, bytesColumn);
    }

    /**
     * Reads and transforms the value of the column in the given record from the original Java value
     * into one suitable for storing in the database.
     *
     * @param record
     *         the record with column
     * @param name
     *         the name of the column which value should be obtained
     */
    @Nullable Object valueIn(RecordWithColumns<I, R> record, ColumnName name) {
        var column = requireColumn(name);
        @Nullable Object result = column.valueIn(record);
        return result;
    }

    private TableColumn requireColumn(ColumnName name) {
        var column = dataColumns.get(name);
        requireNonNull(column,
                       format("Cannot find the column with name `%s` " +
                                      "in the table specification for the record of type `%s` " +
                                      "which columns are sourced from type `%s`.",
                              name, recordSpec.recordType(), recordSpec.sourceType()));
        return column;
    }

    /**
     * Returns the original specification of the stored record.
     */
    RecordSpec<I, R> recordSpec() {
        return recordSpec;
    }
}
