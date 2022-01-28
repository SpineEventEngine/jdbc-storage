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

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.InsertSingleRecord;
import io.spine.server.storage.jdbc.query.UpdateSingleRecord;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.RecordTable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Writes a single record to the database.
 *
 * <p>This is a generic version of the operation, which performs an additional {@code SELECT} lookup
 * before either executing an {@code UPDATE} query or an {@code INSERT} query.
 *
 * @param <I>
 *         the type of the identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
public class WriteOne<I, R extends Message> extends Operation<I, R> {

    /**
     * Creates a new operation.
     *
     * @param table
     *         table to write the records to
     * @param dataSource
     *         the data source to use for connectivity
     */
    protected WriteOne(RecordTable<I, R> table, DataSourceWrapper dataSource) {
        super(table, dataSource);
    }

    /**
     * Stores the given record to the database.
     *
     * @param record
     *         a record to store
     */
    public void execute(JdbcRecord<I, R> record) {
        checkNotNull(record);
        if (containsRecord(record.id())) {
            newUpdate(record).execute();
        } else {
            newInsert(record).execute();
        }
    }

    /**
     * Checks if the table contains a record with given ID.
     *
     * @param id
     *         an ID to check
     * @return {@code true} if there is a record with such ID in the table,
     *         {@code false} otherwise
     */
    protected final boolean containsRecord(I id) {
        var query = newContainsQuery(id);
        boolean result = query.execute();
        return result;
    }

    protected final UpdateSingleRecord<I, R> newUpdate(JdbcRecord<I, R> record) {
        var id = record.id();
        UpdateSingleRecord.Builder<I, R> builder = UpdateSingleRecord.newBuilder();
        var query = builder.setTableSpec(table().spec())
                           .setDataSource(dataSource())
                           .setIdColumn(idColumn())
                           .setId(id)
                           .setRecord(record)
                           .build();
        return query;
    }

    protected final InsertSingleRecord<I, R> newInsert(JdbcRecord<I, R> record) {
        var id = record.id();
        InsertSingleRecord.Builder<I, R> builder = InsertSingleRecord.newBuilder();
        var query = builder.setTableSpec(table().spec())
                           .setDataSource(dataSource())
                           .setIdColumn(idColumn())
                           .setId(id)
                           .setRecord(record)
                           .build();
        return query;
    }
}
