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

package io.spine.server.storage.jdbc.message;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.Expression;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.dml.SQLInsertClause;

import static io.spine.server.storage.jdbc.query.MysqlExtensions.withOnDuplicateUpdate;

/**
 * An optimized query which inserts a record corresponding to a single Proto message
 * into the table, or updates if one already exists.
 *
 * <p>This query uses {@code INSERT ... ON DUPLICATE KEY UPDATE ...} clause.
 *
 * @param <I>
 *         the type of table identifiers
 * @param <M>
 *         the type of messages to insert or update
 */
final class InsertOrUpdateSingleMessage<I, M extends Message> extends WriteSingleMessage<I, M> {

    private InsertOrUpdateSingleMessage(Builder<I, M> builder) {
        super(builder);
    }

    @Override
    protected StoreClause<?> clause() {
        SQLInsertClause original = factory().insert(table());
        SQLInsertClause result = original.set(idPath(), normalizedId());
        return result;
    }

    /**
     * Extends the query with {@code ON DUPLICATE KEY UPDATE ...} clause,
     * and sets the corresponding values for the columns to update.
     */
    @Override
    protected void extendClause(StoreClause<?> query, M message) {
        SQLInsertClause insert = (SQLInsertClause) query;

        ImmutableList.Builder<Expression<?>> expBuilder = ImmutableList.builder();
        addIdExpression(expBuilder);
        populateColumnExpressions(message, expBuilder);
        ImmutableList<Expression<?>> expressions = expBuilder.build();

        withOnDuplicateUpdate(insert, expressions);
    }

    private void addIdExpression(ImmutableList.Builder<Expression<?>> expBuilder) {
        Expression<Object> setId = SQLExpressions.set(idPath(), normalizedId());
        expBuilder.add(setId);
    }

    private void populateColumnExpressions(M message,
                                           ImmutableList.Builder<Expression<?>> expBuilder) {
        for (MessageTable.Column<M> column : columns()) {
            if (isIdColumn(column)) {
                continue;
            }
            Object columnValue = column.getter()
                                       .apply(message);
            Expression<Object> setColumn = SQLExpressions.set(pathOf(column), columnValue);
            expBuilder.add(setColumn);
        }
    }

    /**
     * Returns a new builder for this query.
     */
    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    /**
     * A builder of {@code InsertOrUpdateSingleMessage} queries.
     *
     * @param <I>
     *         the type of table identifiers
     * @param <M>
     *         the type of messages to insert or update
     */
    static class Builder<I, M extends Message>
            extends WriteSingleMessage.Builder<I,
                                               M,
                                               Builder<I, M>,
                                               InsertOrUpdateSingleMessage<I, M>> {

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected InsertOrUpdateSingleMessage<I, M> doBuild() {
            return new InsertOrUpdateSingleMessage<>(this);
        }
    }
}
