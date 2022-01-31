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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.query.RecordQuery;
import io.spine.query.SortBy;
import io.spine.server.entity.FieldMasks;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.ResultSet;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static io.spine.query.Direction.ASC;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;
import static io.spine.server.storage.jdbc.query.QueryPredicates.inIds;
import static io.spine.server.storage.jdbc.query.QueryPredicates.matchPredicate;
import static io.spine.server.storage.jdbc.record.column.BytesColumn.bytesColumnName;
import static java.util.Objects.requireNonNull;

/**
 * Selects multiple records from the {@link RecordTable RecordTable}
 * by the passed {@link RecordQuery}.
 *
 * @param <I>
 *         the type of identifiers of the queried records
 * @param <R>
 *         the type of queried records
 */
public class SelectMessagesByQuery<I, R extends Message> extends AbstractQuery<I, R>
        implements SelectQuery<Iterator<R>> {

    private final RecordQuery<I, R> recordQuery;
    private final JdbcColumnMapping columnMapping;
    private final Descriptor descriptor;

    private SelectMessagesByQuery(Builder<I, R> builder) {
        super(builder);
        this.recordQuery = builder.recordQuery;
        var tableSpec = requireNonNull(builder.tableSpec());
        this.columnMapping = tableSpec.columnMapping();
        this.descriptor = tableSpec.recordDescriptor();
    }

    @Override
    public Iterator<R> execute() {
        var subject = recordQuery.subject();
        var idColumn = tableSpec().idColumn();
        var inIds = inIds(idColumn, subject.id().values());
        var matchParameters = matchPredicate(subject.predicate(), columnMapping);

        var query = factory().select(pathOf(bytesColumnName()))
                             .where(inIds)
                             .where(matchParameters)
                             .from(table());
        addSorting(query, recordQuery.sorting());
        setLimit(query);

        var resultSet = query.getResults();
        var records = asIterator(resultSet);
        var maskedRecords = maskFields(records);
        return maskedRecords;
    }

    private void setLimit(AbstractSQLQuery<Object, ? extends AbstractSQLQuery<Object, ?>> query) {
        var limit = recordQuery.limit();
        if(limit != null && limit >= 0) {
            query.limit(limit);
        }
    }

    private void addSorting(AbstractSQLQuery<Object, ?> query, Iterable<SortBy<?, R>> sorting) {
        for (var sortDirective : sorting) {
            var column = sortDirective.column();
            var name = column.name().value();
            var sortingPath = comparablePath(Comparable.class, name);
            var order = sortDirective.direction() == ASC ? Order.ASC : Order.DESC;
            var specifier = new OrderSpecifier<>(order, sortingPath);
            query.orderBy(specifier);
        }
    }

    private Iterator<R> asIterator(ResultSet resultSet) {
        MessageBytesColumnReader<R> messageReader = messageReader(bytesColumnName(), descriptor);
        var records = DbIterator.over(resultSet, messageReader);
        var result = ImmutableList.copyOf(records);
        return result.iterator();
    }

    @NonNull
    private Iterator<R> maskFields(Iterator<R> records) {
        var mask = recordQuery.mask();
        Iterator<R> result;
        if (!mask.equals(FieldMask.getDefaultInstance())) {
            result = Iterators.transform(records, record -> FieldMasks.applyMask(mask, record));
        } else {
            result = records;
        }
        return result;
    }

    /**
     * Creates and returns a new builder for {@code SelectMessagesByQuery}.
     *
     * @param <I>
     *         the type of identifiers of the queried records
     * @param <R>the
     *         type of queried records
     * @return a new {@code Builder} instance
     */
    public static <I, R extends Message> Builder<I, R> newBuilder() {
        return new Builder<>();
    }

    /**
     * A builder of {@code SelectMessagesByQuery}.
     *
     * @param <I>
     *         the type of identifiers of the queried records
     * @param <R>
     *         the type of queried records
     */
    public static class Builder<I, R extends Message>
            extends AbstractQuery.Builder<I, R, Builder<I, R>, SelectMessagesByQuery<I, R>> {

        private RecordQuery<I, R> recordQuery;

        private Builder() {
            super();
        }

        public Builder<I, R> setQuery(RecordQuery<I, R> recordQuery) {
            this.recordQuery = checkNotNull(recordQuery);
            return this;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Checks that all the builder fields were set to a non-{@code null} values.
         */
        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkNotNull(recordQuery, "`RecordQuery` must be set.");
        }

        @Override
        protected SelectMessagesByQuery<I, R> doBuild() {
            return new SelectMessagesByQuery<>(this);
        }

        @Override
        protected SelectMessagesByQuery.Builder<I, R> getThis() {
            return this;
        }
    }
}
