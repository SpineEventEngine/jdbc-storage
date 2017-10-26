/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.query.dsl;

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.sql.AbstractSQLQuery;
import com.querydsl.sql.StatementOptions;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DbIterator;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.MessageDbIterator;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;

import java.sql.ResultSet;

import static com.querydsl.core.types.Order.DESC;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.aggregate;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.timestamp;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.timestamp_nanos;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.version;
import static io.spine.server.storage.jdbc.EventCountTable.Column.id;
import static io.spine.type.TypeUrl.of;

/**
 * Query that selects {@linkplain AggregateEventRecord event records} by an aggregate ID.
 *
 * <p>Resulting records is ordered by version descending. If the version is the same for
 * several records, they will be ordered by creation time descending.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class SelectEventRecordsById<I> extends AbstractQuery
        implements SelectByIdQuery<I, DbIterator<AggregateEventRecord>> {

    private final IdColumn<I> idColumn;
    private final I idValue;
    private final int fetchSize;

    private SelectEventRecordsById(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.idValue = builder.id;
        this.fetchSize = builder.fetchSize;
    }

    @Override
    public DbIterator<AggregateEventRecord> execute() {
        final BooleanExpression hasId = pathOf(id).eq(idColumn.normalize(idValue));
        final OrderSpecifier<Comparable> byVersion = orderBy(version, DESC);
        final OrderSpecifier<Comparable> bySeconds = orderBy(timestamp, DESC);
        final OrderSpecifier<Comparable> byNanos = orderBy(timestamp_nanos, DESC);

        final AbstractSQLQuery<Object, ?> query = factory().select(pathOf(aggregate))
                                                           .from(table())
                                                           .where(hasId)
                                                           .orderBy(byVersion, bySeconds, byNanos);
        query.setStatementOptions(StatementOptions.builder()
                                                  .setFetchSize(fetchSize)
                                                  .build());
        final ResultSet resultSet = query.getResults();
        return new MessageDbIterator<>(resultSet,
                                       aggregate.name(),
                                       of(AggregateEventRecord.class));
    }

    @Override
    boolean closeConnectionAfterExecution() {
        return false;
    }

    @Override
    public IdColumn<I> getIdColumn() {
        return idColumn;
    }

    @Override
    public I getId() {
        return idValue;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>,
                                                          SelectEventRecordsById<I>> {

        private IdColumn<I> idColumn;
        private I id;
        private int fetchSize;

        Builder<I> setId(I id) {
            this.id = id;
            return getThis();
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        Builder<I> setFetchSize(int batchSize) {
            this.fetchSize = batchSize;
            return getThis();
        }

        @Override
        public SelectEventRecordsById<I> build() {
            return new SelectEventRecordsById<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
