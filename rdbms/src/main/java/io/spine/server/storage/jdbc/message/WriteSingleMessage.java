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

package io.spine.server.storage.jdbc.message;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.server.storage.jdbc.message.MessageTable.Column;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.IdColumn;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract base for queries that store a single message to a {@link MessageTable}.
 *
 * @param <I>
 *         the record ID type
 * @param <M>
 *         the message type
 */
abstract class WriteSingleMessage<I, M extends Message>
        extends IdAwareQuery<I>
        implements WriteMessageQuery<I, M> {

    private final M message;
    private final ImmutableList<? extends Column<M>> columns;

    WriteSingleMessage(Builder<I, M, ? extends Builder, ? extends WriteSingleMessage> builder) {
        super(builder);
        this.message = checkNotNull(builder.message);
        this.columns = checkNotNull(builder.columns);
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        StoreClause<?> query = clause();
        setColumnValues(query, message);
        return query.execute();
    }

    /**
     * Obtains an SQL clause to use, basically {@code INSERT} or {@code UPDATE}.
     */
    protected abstract StoreClause<?> clause();

    @Override
    public Iterable<? extends Column<M>> columns() {
        return columns;
    }

    @Override
    public IdColumn<I> idColumn() {
        return super.idColumn();
    }

    @Override
    public PathBuilder<Object> pathOf(Column<M> column) {
        return super.pathOf(column);
    }

    abstract static class Builder<I,
                                  M extends Message,
                                  B extends Builder<I, M, B, Q>,
                                  Q extends WriteSingleMessage<I, M>>
            extends IdAwareQuery.Builder<I, B, Q> {

        private M message;
        private ImmutableList<? extends Column<M>> columns;

        B setMessage(M message) {
            this.message = message;
            return getThis();
        }

        B setColumns(Iterable<? extends Column<M>> columns) {
            this.columns = ImmutableList.copyOf(columns);
            return getThis();
        }
    }
}
