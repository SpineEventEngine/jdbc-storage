/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.message;

import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.server.storage.jdbc.message.MessageTable.Column;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;

/**
 * A common interface for {@linkplain io.spine.server.storage.jdbc.query.AbstractQuery queries}
 * that write one or more messages to the {@link MessageTable}.
 */
interface WriteMessageQuery<I, M extends Message> extends WriteQuery {

    default void setColumnValues(StoreClause<?> query, M record) {
        checkNotNull(query);
        checkNotNull(record);
        stream(columns())
                .filter(column -> !isIdColumn(column))
                .forEach(column -> setColumnValue(query, column, record));
    }

    default boolean isIdColumn(Column<M> column) {
        checkNotNull(column);
        return idColumn().column()
                         .equals(column);
    }

    default void setColumnValue(StoreClause<?> query, Column<M> column, M record) {
        checkNotNull(query);
        checkNotNull(column);
        checkNotNull(record);

        query.set(pathOf(column), column.getter()
                                        .apply(record));
    }

    Iterable<? extends Column<M>> columns();

    IdColumn<I> idColumn();

    PathBuilder<Object> pathOf(Column<M> column);
}
