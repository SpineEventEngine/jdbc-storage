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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Timestamp;
import com.querydsl.sql.dml.SQLInsertClause;
import io.spine.core.Event;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static io.spine.protobuf.Messages.isDefault;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.AGGREGATE;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.KIND;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP_NANOS;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.VERSION;
import static io.spine.server.storage.jdbc.query.Serializer.serialize;

/**
 * A query that inserts a new {@link AggregateEventRecord} into
 * the {@link AggregateEventRecordTable}.
 */
final class InsertAggregateRecordQuery<I> extends IdAwareQuery<I> implements WriteQuery {

    private final AggregateEventRecord record;

    private InsertAggregateRecordQuery(Builder<I> builder) {
        super(builder);
        this.record = builder.record;
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        Timestamp recordTimestamp = record.getTimestamp();
        String kindValue = record.getKindCase()
                                 .toString();
        SQLInsertClause query = insertWithId()
                .set(pathOf(AGGREGATE), serialize(record))
                .set(pathOf(KIND), kindValue)
                .set(pathOf(VERSION), getVersionNumberOfRecord())
                .set(pathOf(TIMESTAMP), recordTimestamp.getSeconds())
                .set(pathOf(TIMESTAMP_NANOS), recordTimestamp.getNanos());
        return query.execute();
    }

    private int getVersionNumberOfRecord() {
        int versionNumber;

        Event event = record.getEvent();
        if (isDefault(event)) {
            versionNumber = record.getSnapshot()
                                  .getVersion()
                                  .getNumber();
        } else {
            versionNumber = event.getContext()
                                 .getVersion()
                                 .getNumber();
        }
        return versionNumber;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends IdAwareQuery.Builder<I,
                                                         Builder<I>,
                                                         InsertAggregateRecordQuery<I>> {

        private AggregateEventRecord record;

        Builder<I> setRecord(AggregateEventRecord record) {
            this.record = record;
            return getThis();
        }

        @Override
        protected InsertAggregateRecordQuery<I> doBuild() {
            return new InsertAggregateRecordQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
