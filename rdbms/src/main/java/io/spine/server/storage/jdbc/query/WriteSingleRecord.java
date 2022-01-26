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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.query.ColumnName;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.NewRecordTable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract base for queries that write
 * a single record to a {@link NewRecordTable}.
 *
 * //TODO:2022-01-17:alex.tymchenko: move this type.
 *
 * @param <I>
 *         the record ID type
 * @param <R>
 *         the record type
 */
abstract class WriteSingleRecord<I, R extends Message>
        extends IdAwareQuery<I, R>
        implements WriteMessageQuery<I, R> {

    private final JdbcRecord<I, R> record;

    WriteSingleRecord(
            Builder<I, R, ? extends Builder<I, R, ?, ?>, ? extends WriteSingleRecord<I, R>> b) {
        super(b);
        this.record = checkNotNull(b.record);
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        var query = clause();
        setColumnValues(query, record);
        return query.execute();
    }

    /**
     * Obtains an SQL clause to use, basically {@code INSERT} or {@code UPDATE}.
     */
    protected abstract StoreClause<?> clause();

    @Override
    public IdColumn<I> idColumn() {
        return super.idColumn();
    }

    @Override
    public PathBuilder<Object> pathOf(TableColumn column) {
        return super.pathOf(column);
    }

    @Override
    public PathBuilder<Object> pathOf(ColumnName name) {
        return super.pathOf(name);
    }

    abstract static class Builder<I,
                                  R extends Message,
                                  B extends Builder<I, R, B, Q>,
                                  Q extends WriteSingleRecord<I, R>>
            extends IdAwareQuery.Builder<I, R, B, Q> {

        private JdbcRecord<I, R> record;

        public B setRecord(JdbcRecord<I, R> record) {
            this.record = record;
            return getThis();
        }
    }
}
