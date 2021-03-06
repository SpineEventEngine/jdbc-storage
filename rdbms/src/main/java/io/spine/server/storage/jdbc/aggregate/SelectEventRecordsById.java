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

package io.spine.server.storage.jdbc.aggregate;

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import com.querydsl.sql.StatementOptions;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.query.ColumnReader;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;

import static com.querydsl.core.types.Order.DESC;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.AGGREGATE;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP_NANOS;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.VERSION;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

/**
 * A query that selects {@linkplain AggregateEventRecord event records} by an aggregate ID.
 *
 * <p>Resulting records are ordered by version descending. If the version is the same for
 * several records, they will be ordered by creation time descending.
 */
final class SelectEventRecordsById<I>
        extends IdAwareQuery<I>
        implements SelectQuery<DbIterator<AggregateEventRecord>> {

    private SelectEventRecordsById(Builder<I> builder) {
        super(builder);
    }

    public DbIterator<AggregateEventRecord> execute(int fetchSize) {
        OrderSpecifier<Comparable> byVersion = orderBy(VERSION, DESC);
        OrderSpecifier<Comparable> bySeconds = orderBy(TIMESTAMP, DESC);
        OrderSpecifier<Comparable> byNanos = orderBy(TIMESTAMP_NANOS, DESC);

        AbstractSQLQuery<Object, ?> query = factory()
                .select(pathOf(AGGREGATE))
                .from(table())
                .where(idEquals())
                .orderBy(byVersion, bySeconds, byNanos);
        query.setStatementOptions(StatementOptions.builder()
                                                  .setFetchSize(fetchSize)
                                                  .build());
        ResultSet resultSet = query.getResults();

        ColumnReader<AggregateEventRecord> aggregateColumnReader =
                messageReader(AGGREGATE.name(), AggregateEventRecord.getDescriptor());
        DbIterator<AggregateEventRecord> dbIterator =
                DbIterator.over(resultSet, aggregateColumnReader);
        return dbIterator;
    }

    @Override
    public DbIterator<AggregateEventRecord> execute() {
        int defaultFetchSize = 0;
        return execute(defaultFetchSize);
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends IdAwareQuery.Builder<I,
                                                         Builder<I>,
                                                         SelectEventRecordsById<I>> {

        @Override
        protected SelectEventRecordsById<I> doBuild() {
            return new SelectEventRecordsById<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
