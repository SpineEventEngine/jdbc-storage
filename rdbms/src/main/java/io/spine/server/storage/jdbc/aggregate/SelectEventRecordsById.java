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

package io.spine.server.storage.jdbc.aggregate;

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.query.AbstractSelectByIdQuery;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.MessageDbIterator;

import java.sql.ResultSet;

import static com.querydsl.core.types.Order.DESC;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.aggregate;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.timestamp;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.timestamp_nanos;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.version;

/**
 * Query that selects {@linkplain AggregateEventRecord event records} by an aggregate ID.
 *
 * <p>Resulting records is ordered by version descending. If the version is the same for
 * several records, they will be ordered by creation time descending.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class SelectEventRecordsById<I> extends AbstractSelectByIdQuery<I, DbIterator<AggregateEventRecord>> {

    private SelectEventRecordsById(Builder<I> builder) {
        super(builder);
    }

    @Override
    public DbIterator<AggregateEventRecord> execute() {
        final OrderSpecifier<Comparable> byVersion = orderBy(version, DESC);
        final OrderSpecifier<Comparable> bySeconds = orderBy(timestamp, DESC);
        final OrderSpecifier<Comparable> byNanos = orderBy(timestamp_nanos, DESC);

        final AbstractSQLQuery<Object, ?> query = factory().select(pathOf(aggregate))
                                                           .from(table())
                                                           .where(hasId())
                                                           .orderBy(byVersion, bySeconds, byNanos);
        final ResultSet resultSet = query.getResults();
        return new MessageDbIterator<>(resultSet,
                                       aggregate.name(),
                                       AggregateEventRecord.getDescriptor());
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractSelectByIdQuery.Builder<I,
                                                                    Builder<I>,
                                                                    SelectEventRecordsById<I>> {

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
