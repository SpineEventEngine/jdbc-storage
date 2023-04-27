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

package io.spine.server.storage.jdbc.record;

import com.google.common.collect.ImmutableList;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.dml.SQLInsertClause;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.Parameters;

import java.util.Set;

import static io.spine.server.storage.jdbc.query.MysqlExtensions.withOnDuplicateUpdate;
import static io.spine.server.storage.jdbc.query.Serializer.serialize;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ENTITY;

final class InsertOrUpdateEntityQuery<I> extends WriteEntityQuery<I, SQLInsertClause> {

    private InsertOrUpdateEntityQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    protected void addBatch(SQLInsertClause clause) {
        clause.addBatch();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @Override
    protected void setIdValue(SQLInsertClause clause, IdColumn<I> idColumn, Object normalizedId) {
        PathBuilder<Object> idPath = pathOf(idColumn);
        clause.set(idPath, normalizedId);
    }

    @Override
    protected SQLInsertClause createClause() {
        SQLInsertClause clause = factory().insert(table());
        return clause;
    }

    /**
     * Extends the query with {@code ON DUPLICATE KEY UPDATE ...} clause,
     * and sets the corresponding values for the columns to update.
     */
    @Override
    protected void extendClause(SQLInsertClause clause, I id, EntityRecordWithColumns record) {
        ImmutableList.Builder<Expression<?>> expBuilder = ImmutableList.builder();
        addEntityExpression(record, expBuilder);
        addColumnExpressions(record, expBuilder);
        addIdExpression(id, expBuilder);
        ImmutableList<Expression<?>> expressions = expBuilder.build();
        withOnDuplicateUpdate(clause, expressions);
    }

    private void addIdExpression(I id, ImmutableList.Builder<Expression<?>> expBuilder) {
        IdColumn<I> idColumn = idColumn();
        Object normalizedId = idColumn.normalize(id);
        PathBuilder<Object> idPath = pathOf(idColumn);
        Expression<Object> setId = SQLExpressions.set(idPath, normalizedId);
        expBuilder.add(setId);
    }

    private void addEntityExpression(EntityRecordWithColumns record,
                           ImmutableList.Builder<Expression<?>> expBuilder) {
        byte[] serializedRecord = serialize(record.record());
        Expression<Object> setEntity = SQLExpressions.set(pathOf(ENTITY), serializedRecord);
        expBuilder.add(setEntity);
    }

    private void addColumnExpressions(EntityRecordWithColumns record,
                                      ImmutableList.Builder<Expression<?>> expBuilder) {
        Parameters parameters = createParametersFromColumns(record);
        Set<String> identifiers = parameters.getIdentifiers();
        for (String identifier : identifiers) {
            Object parameterValue = parameters.getParameter(identifier)
                                              .getValue();
            Expression<Object> setParameter =
                    SQLExpressions.set(pathOf(identifier), parameterValue);
            expBuilder.add(setParameter);
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName") /* By design. */
    static class Builder<I>
            extends WriteEntityQuery.Builder<Builder<I>, InsertOrUpdateEntityQuery<I>, I> {

        @Override
        protected InsertOrUpdateEntityQuery<I> doBuild() {
            return new InsertOrUpdateEntityQuery<>(this);
        }

        @Override
        protected InsertOrUpdateEntityQuery.Builder<I> getThis() {
            return this;
        }
    }
}
