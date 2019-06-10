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

package io.spine.server.storage.jdbc.aggregate;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.ColumnReader;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.DbIterator.DoubleColumnRecord;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.querydsl.core.types.Order.DESC;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.AGGREGATE;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.ID;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP_NANOS;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.VERSION;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idReader;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

/**
 * Selects all aggregate event records along with their IDs from the storage.
 *
 * @param <I>
 *         the type of the record IDs
 */
final class EventRecordsWithIdQuery<I>
        extends AbstractQuery
        implements SelectQuery<DbIterator<DoubleColumnRecord<I, AggregateEventRecord>>> {

    private final IdColumn<I> idColumn;

    private EventRecordsWithIdQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
    }

    @Override
    public DbIterator<DoubleColumnRecord<I, AggregateEventRecord>> execute() {
        OrderSpecifier<Comparable> byVersion = orderBy(VERSION, DESC);
        OrderSpecifier<Comparable> bySeconds = orderBy(TIMESTAMP, DESC);
        OrderSpecifier<Comparable> byNanos = orderBy(TIMESTAMP_NANOS, DESC);

        AbstractSQLQuery<Tuple, ?> query = factory()
                .select(pathOf(ID), pathOf(AGGREGATE))
                .from(table())
                .orderBy(byVersion, bySeconds, byNanos);
        ResultSet resultSet = query.getResults();

        ColumnReader<I> idReader = idReader(idColumn.columnName(), idColumn.javaType());
        ColumnReader<AggregateEventRecord> recordReader =
                messageReader(AGGREGATE.name(), AggregateEventRecord.getDescriptor());
        DbIterator<DoubleColumnRecord<I, AggregateEventRecord>> result =
                DbIterator.over(resultSet, idReader, recordReader);
        return result;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>, EventRecordsWithIdQuery<I>> {

        private IdColumn<I> idColumn;

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return getThis();
        }

        @Override
        protected EventRecordsWithIdQuery<I> doBuild() {
            return new EventRecordsWithIdQuery<I>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
