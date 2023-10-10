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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import io.spine.query.ColumnName;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.RecordTable;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A common interface for {@linkplain io.spine.server.storage.jdbc.query.AbstractQuery queries}
 * that write one or more records to the {@link RecordTable}.
 *
 * @param <I>
 *         type of record identifiers
 * @param <R>
 *         type of records to write
 */
@SuppressWarnings("WeakerAccess" /* Available to SPI users. */)
public abstract class WriteQuery<I, R extends Message> extends ModifyQuery<I, R> {

    protected WriteQuery(
            Builder<I, R, ? extends Builder<I, R, ?, ?>, ? extends StorageQuery<I, R>> builder) {
        super(builder);
    }

    /**
     * Adds a value binding to the {@code query} for each {@code record} field described
     * as a column in the specified {@code JdbcRecord}.
     */
    protected void setColumnValues(StoreClause<?> query, JdbcRecord<I, R> record) {
        checkNotNull(query);
        checkNotNull(record);
        var names = record.columns();
        for (var name : names) {
            var value = record.columnValue(name);
            setColumnValue(query, name, value);
        }
    }

    /**
     * Adds a single value binding to the query, using the passed value for the column
     * by the passed name.
     */
    protected void setColumnValue(StoreClause<?> query, ColumnName column, @Nullable Object value) {
        checkNotNull(query);
        checkNotNull(column);
        query.set(pathOf(column), value);
    }
}
