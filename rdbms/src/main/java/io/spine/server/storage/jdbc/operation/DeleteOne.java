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

package io.spine.server.storage.jdbc.operation;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.DeleteRecordQuery;
import io.spine.server.storage.jdbc.record.RecordTable;

/**
 * Deletes a single record to the database by its identifier.
 *
 * @param <I>
 *         the type of the identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
public class DeleteOne<I, R extends Message> extends Operation<I, R> {

    /**
     * Creates a new operation.
     *
     * @param table
     *         table to delete the record from
     * @param dataSource
     *         the data source to use for connectivity
     */
    protected DeleteOne(RecordTable<I, R> table, DataSourceWrapper dataSource) {
        super(table, dataSource);
    }

    /**
     * Deletes a record in the table corresponding to the given ID.
     *
     * @param id
     *         an ID to search by
     * @return {@code true} if the record was deleted successfully,
     *         {@code false} if the record was not found
     */
    public boolean execute(I id) {
        var query = deleteQuery(id);
        var rowsAffected = query.execute();
        return rowsAffected != 0;
    }

    private DeleteRecordQuery<I, R> deleteQuery(I id) {
        var query = DeleteRecordQuery.<I, R>newBuilder()
                .setTableSpec(table().spec())
                .setDataSource(dataSource())
                .setId(id)
                .build();
        return query;
    }
}
