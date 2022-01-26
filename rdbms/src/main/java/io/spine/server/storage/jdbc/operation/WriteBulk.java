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

package io.spine.server.storage.jdbc.operation;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.InsertRecordsInBulk;
import io.spine.server.storage.jdbc.query.UpdateRecordsInBulk;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.NewRecordTable;

import java.util.Collection;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.util.stream.Collectors.toList;

/**
 * Writes several records to the database in a bulk.
 *
 * <p>This is a generic version of the operation, which performs an additional {@code SELECT} lookup
 * before either executing an {@code UPDATE} query or an {@code INSERT} query.
 *
 * @param <I>
 *         the type of the identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
//TODO:2021-12-23:alex.tymchenko: think of having an abstract base and three impls instead.
public class WriteBulk<I, R extends Message> extends Operation<I, R> {

    private final OperationFactory operations;

    /**
     * Creates a new operation.
     *
     * @param table
     *         table to write the records to
     * @param dataSource
     *         the data source to use for connectivity
     * @param operations
     *         the factory to instantiate auxiliary operations
     */
    protected WriteBulk(NewRecordTable<I, R> table,
                        DataSourceWrapper dataSource,
                        OperationFactory operations) {
        super(table, dataSource);
        this.operations = operations;
    }

    /**
     * Executes this operation.
     */
    public void execute(Iterable<JdbcRecord<I, R>> records) {
        var existingIds = existingIds(records);

        var existingRecords =
                stream(records)
                        .filter(record -> existingIds.contains(record.id()))
                        .collect(toImmutableList());
        var newRecords =
                stream(records)
                        .filter(record -> !existingIds.contains(record.id()))
                        .collect(toImmutableList());

        updateAll(existingRecords);
        insertAll(newRecords);
    }

    private Collection<I> existingIds(Iterable<JdbcRecord<I, R>> records) {
        var ids = stream(records)
                .map(JdbcRecord::id)
                .collect(toList());

        var existingRecords = operations.readManyByIds(table())
                                        .execute(ids);
        var existingIds = stream(existingRecords)
                .map(record -> table().spec()
                                      .idFromRecord(record))
                .collect(toList());
        return existingIds;
    }

    private void insertAll(ImmutableList<JdbcRecord<I, R>> records) {
        var query = newBulkInsert(records);
        query.execute();
    }

    private void updateAll(ImmutableList<JdbcRecord<I, R>> records) {
        var query = newBulkUpdate(records);
        query.execute();
    }

    private InsertRecordsInBulk<I, R>
    newBulkInsert(ImmutableList<JdbcRecord<I, R>> records) {
        InsertRecordsInBulk.Builder<I, R> builder = InsertRecordsInBulk.newBuilder();
        var query = builder.setTableName(tableName())
                           .setDataSource(dataSource())
                           .setIdColumn(idColumn())
                           .setTableSpec(table().spec())
                           .setRecords(records)
                           .build();
        return query;
    }

    private UpdateRecordsInBulk<I, R>
    newBulkUpdate(ImmutableList<JdbcRecord<I, R>> records) {
        UpdateRecordsInBulk.Builder<I, R> builder = UpdateRecordsInBulk.newBuilder();
        var query = builder.setTableName(tableName())
                           .setDataSource(dataSource())
                           .setIdColumn(idColumn())
                           .setRecords(records)
                           .build();
        return query;
    }
}
