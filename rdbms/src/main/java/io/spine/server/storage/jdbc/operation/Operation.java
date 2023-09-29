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
import io.spine.annotation.SPI;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.ContainsQuery;
import io.spine.server.storage.jdbc.record.column.IdColumn;
import io.spine.server.storage.jdbc.record.RecordTable;

/**
 * An I/O operation performed over the DB table.
 *
 * @param <I>
 *         the type of the identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
@SPI
public abstract class Operation<I, R extends Message> {

    private final RecordTable<I, R> table;
    private final DataSourceWrapper dataSource;

    protected Operation(RecordTable<I, R> table,
                        DataSourceWrapper dataSource) {
        this.table = table;
        this.dataSource = dataSource;
    }

    /**
     * Returns the definition of the table over which this operation is executed.
     */
    protected final RecordTable<I, R> table() {
        return table;
    }

    /**
     * Returns the name of the table over which this operation is performed.
     */
    protected final String tableName() {
        return table().name();
    }

    /**
     * Returns the definition of the table column containing record identifiers.
     */
    protected final IdColumn<I> idColumn() {
        return table().idColumn();
    }

    /**
     * Returns the definition of the data source to use.
     */
    public final DataSourceWrapper dataSource() {
        return dataSource;
    }


    protected ContainsQuery<I, R> newContainsQuery(I id) {
        ContainsQuery.Builder<I, R> builder = ContainsQuery.newBuilder();
        var query = builder.setId(id)
                           .setTableSpec(table().spec())
                           .setDataSource(dataSource())
                           .build();
        return query;
    }
}