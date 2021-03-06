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

package io.spine.server.storage.jdbc.record;

import com.google.protobuf.FieldMask;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Predicate;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.DbIterator.DoubleColumnRecord;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import java.sql.ResultSet;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.jdbc.query.QueryPredicates.inIds;
import static io.spine.server.storage.jdbc.query.QueryPredicates.matchParameters;
import static io.spine.server.storage.jdbc.record.QueryResults.parse;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ENTITY;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ID;

/**
 * A query selecting the records from the {@link RecordTable RecordTable} by an {@link EntityQuery}.
 */
final class SelectByEntityColumnsQuery<I> extends AbstractQuery
        implements SelectQuery<Iterator<DoubleColumnRecord<I, EntityRecord>>> {

    private final EntityQuery<I> entityQuery;
    private final FieldMask fieldMask;
    private final JdbcColumnMapping<?> columnMapping;
    private final IdColumn<I> idColumn;

    private SelectByEntityColumnsQuery(Builder<I> builder) {
        super(builder);
        this.entityQuery = builder.entityQuery;
        this.fieldMask = builder.fieldMask;
        this.columnMapping = builder.columnMapping;
        this.idColumn = builder.idColumn;
    }

    @Override
    public Iterator<DoubleColumnRecord<I, EntityRecord>> execute() {
        Predicate inIds = inIds(idColumn, entityQuery.getIds());
        Predicate matchParameters = matchParameters(entityQuery.getParameters(),
                                                    columnMapping);
        AbstractSQLQuery<Tuple, ?> query = factory().select(pathOf(ID), pathOf(ENTITY))
                                                    .where(inIds)
                                                    .where(matchParameters)
                                                    .from(table());
        ResultSet resultSet = query.getResults();
        Class<I> idType = idColumn.javaType();
        return parse(resultSet, idType, fieldMask);
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>,
                                                          SelectByEntityColumnsQuery<I>> {

        private EntityQuery<I> entityQuery;
        private FieldMask fieldMask;
        private JdbcColumnMapping<?> columnMapping;
        private IdColumn<I> idColumn;

        private Builder() {
            super();
        }

        Builder<I> setEntityQuery(EntityQuery<I> entityQuery) {
            this.entityQuery = checkNotNull(entityQuery);
            return this;
        }

        Builder<I> setFieldMask(FieldMask fieldMask) {
            this.fieldMask = checkNotNull(fieldMask);
            return this;
        }

        Builder<I> setColumnMapping(JdbcColumnMapping<?> columnMapping) {
            this.columnMapping = checkNotNull(columnMapping);
            return this;
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return this;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Checks that all the builder fields were set to a non-{@code null} values.
         */
        @SuppressWarnings("MethodWithMoreThanThreeNegations") // Needed for the check.
        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkState(idColumn != null, "`IdColumn` is not set.");
            checkState(fieldMask != null, "`FieldMask` is not set.");
            checkState(entityQuery != null, "`EntityQuery` is not set.");
            checkState(columnMapping != null, "`ColumnMapping` is not set.");
        }

        @Override
        protected SelectByEntityColumnsQuery<I> doBuild() {
            return new SelectByEntityColumnsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
