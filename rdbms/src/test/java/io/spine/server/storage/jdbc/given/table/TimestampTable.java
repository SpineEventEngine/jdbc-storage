/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.given.table;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Timestamp;
import io.spine.query.RecordColumn;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.given.query.SelectRecordId;
import io.spine.server.storage.jdbc.given.query.SelectTimestampById;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import java.sql.ResultSet;

/**
 * Holds {@link Timestamp} records by some ID.
 *
 * <p>Overrides several {@link RecordTable} methods to expose them to tests.
 *
 * @param <I>
 *         the ID type
 */
abstract class TimestampTable<I> extends RecordTable<I, Timestamp> {

    private final RecordSpec<I, Timestamp> timestampSpec;

    TimestampTable(String name,
                   RecordSpec<I, Timestamp> recordSpec,
                   JdbcStorageFactory factory) {
        super(fullSpecFrom(name, recordSpec, factory.columnMapping()), factory);
        this.timestampSpec = extendRecordSpec(recordSpec);
    }

    private static <I> JdbcTableSpec<I, Timestamp>
    fullSpecFrom(String tableName, RecordSpec<I, Timestamp> spec, JdbcColumnMapping mapping) {
        var updatedSpec = extendRecordSpec(spec);
        var result = new JdbcTableSpec<>(tableName, updatedSpec, mapping);
        return result;
    }

    private static <I> RecordSpec<I, Timestamp> extendRecordSpec(RecordSpec<I, Timestamp> spec) {
        var secondsColumn = new RecordColumn<>("SECONDS", Long.class, Timestamp::getSeconds);
        var nanosColumn = new RecordColumn<>("NANOS", Integer.class, Timestamp::getNanos);

        var builder = ImmutableSet.<RecordColumn<Timestamp, ?>>builder();
        builder.add(secondsColumn)
               .add(nanosColumn);
        for (var column : spec.columns()) {
            var cast = asRecordColumn(column);
            builder.add(cast);
        }
        var idType = spec.idType();
        var allCols = builder.build();

        @SuppressWarnings("Immutable" /* Re-using the same lambda as previously. */)
        var updatedSpec = new RecordSpec<>(idType, Timestamp.class,
                                           spec::idValueIn,
                                           allCols);
        return updatedSpec;
    }

    @SuppressWarnings("unchecked")  // As per the contract of `RecordSpec`.
    private static RecordColumn<Timestamp, ?> asRecordColumn(io.spine.query.Column<?, ?> column) {
        return (RecordColumn<Timestamp, ?>) column;
    }

    public void write(Timestamp record) {
        var withCols = RecordWithColumns.create(record, timestampSpec);
        write(withCols);
    }

    public I idOf(Timestamp record) {
        return spec().idFromRecord(record);
    }

    public ResultSet resultSet(I id) {
        var query = composeSelectTimestampById(id);
        var resultSet = query.getResults();
        return resultSet;
    }

    /**
     * Reads a given ID back from the database as a {@link ResultSet}.
     */
    public ResultSet resultSetWithId(I id) {
        var queryBuilder = SelectRecordId
                .<I, Timestamp>newBuilder()
                .setDataSource(dataSource())
                .setTableSpec(spec())
                .setId(id);
        var query = queryBuilder.build();
        var resultSet = query.getResults();
        return resultSet;
    }

    /**
     * Composes a "select-timestamp-by-ID" query.
     */
    public SelectTimestampById<I> composeSelectTimestampById(I id) {
        var builder = SelectTimestampById
                .<I>newBuilder()
                .setDataSource(dataSource())
                .setTableSpec(spec())
                .setId(id);
        var query = builder.build();
        return query;
    }
}
