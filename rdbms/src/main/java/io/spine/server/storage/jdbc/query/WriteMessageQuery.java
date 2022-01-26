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
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.query.ColumnName;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.RecordTable;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A common interface for {@linkplain io.spine.server.storage.jdbc.query.AbstractQuery queries}
 * that write one or more messages to the {@link RecordTable}.
 */
interface WriteMessageQuery<I, R extends Message> extends WriteQuery {

    /**
     * Adds a value binding to the {@code query} for each {@code record} field described
     * as a column in the passed {@link RecordWithColumns}.
     */
    default void setColumnValues(StoreClause<?> query, JdbcRecord<I, R> record) {
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
    default void setColumnValue(StoreClause<?> query, ColumnName column, @Nullable Object value) {
        checkNotNull(query);
        checkNotNull(column);

        query.set(pathOf(column), value);
    }

    /**
     * Obtains the ID column of the table this query is applied to.
     */
    IdColumn<I> idColumn();

    /**
     * Obtains the path of the given {@code column} respective to the processed table.
     */
    PathBuilder<Object> pathOf(TableColumn column);

    /**
     * Obtains the path of the given {@code column} respective to the processed table.
     */
    PathBuilder<Object> pathOf(ColumnName name);
}
