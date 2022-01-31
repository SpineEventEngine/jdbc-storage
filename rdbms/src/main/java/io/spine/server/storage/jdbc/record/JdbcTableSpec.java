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

package io.spine.server.storage.jdbc.record;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.protobuf.Messages;
import io.spine.query.Column;
import io.spine.query.ColumnName;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.TableNames;
import io.spine.server.storage.jdbc.record.column.BytesColumn;
import io.spine.server.storage.jdbc.record.column.CustomColumns;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static io.spine.util.Preconditions2.checkNotEmptyOrBlank;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The specification of the JDBC-accessible database table telling how the record values should
 * be stored.
 *
 * //TODO:2022-01-13:alex.tymchenko: rename the fields from the `recordSpec`.
 *
 * //TODO:2022-01-13:alex.tymchenko: do we need the customization of type conversion here?
 *
 * //TODO:2022-01-13:alex.tymchenko: rename the table.
 *
 * //TODO:2022-01-13:alex.tymchenko: return the `TableColumn`s:
 * //TODO:2022-01-13:alex.tymchenko:  * performing type conversion;
 * //TODO:2022-01-13:alex.tymchenko:  * handling the `bytes` column.
 *
 * @param <I>
 *         the type of identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
public final class JdbcTableSpec<I, R extends Message> {

    private final RecordSpec<I, R, ?> recordSpec;
    private final JdbcColumnMapping columnMapping;
    private final IdColumn<I> idColumn;
    private final Descriptor recordDescriptor;
    private final @Nullable CustomColumns<R> customColumns;

    private @MonotonicNonNull String tableName;
    private @MonotonicNonNull ImmutableMap<ColumnName, TableColumn> dataColumns;


    //TODO:2022-01-25:alex.tymchenko: move the ID column here?
    public JdbcTableSpec(RecordSpec<I, R, ?> recordSpec,
                         JdbcColumnMapping mapping,
                         @Nullable CustomColumns<R> columnSpecs) {
        this.recordSpec = recordSpec;
        columnMapping = mapping;
        this.customColumns = columnSpecs;
        this.idColumn = IdColumn.of(recordSpec, columnMapping);
        this.recordDescriptor = descriptorFrom(recordSpec.storedType());
    }

    //TODO:2022-01-28:alex.tymchenko: consolidate the ctors.
    public JdbcTableSpec(String tableName,
                         RecordSpec<I, R, ?> recordSpec,
                         JdbcColumnMapping mapping,
                         @Nullable CustomColumns<R> columnSpecs) {
        this.tableName = checkNotEmptyOrBlank(tableName);
        this.recordSpec = recordSpec;
        columnMapping = mapping;
        this.customColumns = columnSpecs;
        this.idColumn = IdColumn.of(recordSpec, columnMapping);
        this.recordDescriptor = descriptorFrom(recordSpec.storedType());
    }

    private static Descriptor descriptorFrom(Class<? extends Message> type) {
        return Messages.defaultInstance(type)
                       .getDescriptorForType();
    }

    public String tableName() {
        if (tableName == null) {
            tableName = defaultName();
        }
        return tableName;
    }

    /**
     * Composes the default name for the table basing on the name of the Java class
     * of the object, on top of which the stored {@link RecordWithColumns} is built.
     *
     * @see TableNames#of(Class) for more details
     */
    private String defaultName() {
        return TableNames.of(sourceType());
    }

    @CanIgnoreReturnValue
    public JdbcTableSpec<I, R> tableName(String value) {
        checkNotEmptyOrBlank(value);
        this.tableName = value;
        return this;
    }

    public Class<R> storedType() {
        return recordSpec.storedType();
    }

    public IdColumn<I> idColumn() {
        return idColumn;
    }

    public I idFromRecord(R record) {
        return recordSpec.idFromRecord(record);
    }

    public Descriptor recordDescriptor() {
        return recordDescriptor;
    }

    public JdbcColumnMapping columnMapping() {
        return columnMapping;
    }

    public ImmutableCollection<TableColumn> dataColumns() {
        if (dataColumns == null) {
            dataColumns = createDataColumns();
        }
        return dataColumns.values();
    }

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
        var customSpec = customColumns != null
                         ? customColumns.find(name)
                         : null;
        TableColumn nativeColumn;
        if (customSpec != null) {
            nativeColumn = new TableColumn(
                    name.value(), column.type(), columnMapping, customSpec.transformFn());
        } else {
            nativeColumn = new TableColumn(name.value(), column.type(), columnMapping);
        }
        return nativeColumn;
    }

    private void addBytesColumn(Map<ColumnName, TableColumn> cols) {
        var bytesColumnName = ColumnName.of(BytesColumn.bytesColumnName());
        var bytesColumn = new BytesColumn(columnMapping);
        cols.put(bytesColumnName, bytesColumn);
    }

    Function<RecordWithColumns<I, R>, Object> transforming(ColumnName name) {
        var column = requireColumn(name);
        return column::valueIn;
    }

    private Class<?> sourceType() {
        return recordSpec.sourceType();
    }

    private TableColumn requireColumn(ColumnName name) {
        var column = dataColumns.get(name);
        requireNonNull(column,
                       format("Cannot find the column with name `%s` " +
                                      "in the table specification for the record of type `%s`.",
                              name, storedType()));
        return column;
    }

}
