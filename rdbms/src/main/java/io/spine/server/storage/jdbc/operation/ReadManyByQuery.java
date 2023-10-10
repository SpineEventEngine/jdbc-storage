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
import io.spine.query.RecordQuery;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.SelectMessagesByQuery;
import io.spine.server.storage.jdbc.record.RecordTable;

import java.util.Iterator;

/**
 * Reads the data from a database table according to the specified {@link RecordQuery}.
 *
 * @param <I>
 *         the type of the identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
public class ReadManyByQuery<I, R extends Message> extends Operation<I, R> {

    /**
     * Creates a new operation.
     *
     * @param table
     *         table to write the records to
     * @param dataSource
     *         the data source to use for connectivity
     */
    @SuppressWarnings("WeakerAccess" /* Available to SPI users. */)
    public ReadManyByQuery(RecordTable<I, R> table, DataSourceWrapper dataSource) {
        super(table, dataSource);
    }

    /**
     * Reads the records by the given query.
     */
    public Iterator<R> execute(RecordQuery<I, R> query) {
        SelectMessagesByQuery.Builder<I, R> builder = SelectMessagesByQuery.newBuilder();
        var sqlQuery = builder.setDataSource(dataSource())
                              .setTableSpec(table().spec())
                              .setQuery(query)
                              .build();
        var queryResult = sqlQuery.execute();
        return queryResult;
    }
}
