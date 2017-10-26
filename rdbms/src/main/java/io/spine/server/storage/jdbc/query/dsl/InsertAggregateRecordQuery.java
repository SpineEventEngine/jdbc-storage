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

import com.google.protobuf.Timestamp;
import io.spine.core.Event;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.AggregateEventRecordTable;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.Serializer;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.aggregate;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.id;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.timestamp;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.timestamp_nanos;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.version;
import static io.spine.validate.Validate.isDefault;

/**
 * Query that inserts a new {@link AggregateEventRecord} to the {@link AggregateEventRecordTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class InsertAggregateRecordQuery<I> extends AbstractQuery implements WriteQuery {

    private final I idValue;
    private final AggregateEventRecord record;
    private final IdColumn<I> idColumn;

    private InsertAggregateRecordQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.idValue = builder.id;
        this.record = builder.record;
    }

    @Override
    public void execute() {
        final Timestamp recordTimestamp = record.getTimestamp();
        factory().insert(table())
                 .set(pathOf(id), idColumn.normalize(idValue))
                 .set(pathOf(aggregate), Serializer.serialize(record))
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

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>,
                                                          InsertAggregateRecordQuery> {

        private IdColumn<I> idColumn;
        private I id;
        private AggregateEventRecord record;

        Builder<I> setId(I id) {
            this.id = id;
            return getThis();
        }

        Builder<I> setRecord(AggregateEventRecord record) {
            this.record = record;
            return getThis();
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        @Override
        public InsertAggregateRecordQuery<I> build() {
            return new InsertAggregateRecordQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
