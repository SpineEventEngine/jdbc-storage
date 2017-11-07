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

import com.google.protobuf.Timestamp;
import io.spine.core.Event;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.aggregate;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.timestamp;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.timestamp_nanos;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.version;
import static io.spine.server.storage.jdbc.query.Serializer.serialize;
import static io.spine.validate.Validate.isDefault;

/**
 * A query that inserts a new {@link AggregateEventRecord} into
 * the {@link AggregateEventRecordTable}.
 *
 * @author Dmytro Grankin
 */
class InsertAggregateRecordQuery<I> extends IdAwareQuery<I> implements WriteQuery {

    private final AggregateEventRecord record;

    private InsertAggregateRecordQuery(Builder<I> builder) {
        super(builder);
        this.record = builder.record;
    }

    @Override
    public long execute() {
        final Timestamp recordTimestamp = record.getTimestamp();
        return factory().insert(table())
                        .set(idPath(), getNormalizedId())
                        .set(pathOf(aggregate), serialize(record))
                        .set(pathOf(timestamp), recordTimestamp.getSeconds())
                        .set(pathOf(timestamp_nanos), recordTimestamp.getNanos())
                        .set(pathOf(version), getVersionNumberOfRecord())
                        .execute();
    }

    private int getVersionNumberOfRecord() {
        final int versionNumber;

        final Event event = record.getEvent();
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
