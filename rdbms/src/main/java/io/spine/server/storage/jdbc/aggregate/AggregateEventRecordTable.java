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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Timestamp;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.WriteQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A table for storing the {@linkplain AggregateEventRecord aggregate event records}.
 */
class AggregateEventRecordTable<I> extends EntityTable<I,
                                                       DbIterator<AggregateEventRecord>,
                                                       AggregateEventRecord> {

    AggregateEventRecordTable(Class<? extends Aggregate<I, ?, ?>> entityClass,
                              DataSourceWrapper dataSource,
                              TypeMapping typeMapping) {
        super(entityClass, Column.ID, dataSource, typeMapping);
    }

    @Override
    protected List<? extends TableColumn> tableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    /**
     * Deletes aggregate event records that are older than the specified {@code version}.
     *
     * @return how many table rows have been deleted
     */
    @CanIgnoreReturnValue
    long deletePriorRecords(I id, Integer version) {
        DeletePriorRecordsQuery<I> query = composeDeleteQuery(id, version);
        return query.execute();
    }

    @Override
    protected SelectEventRecordsById<I> composeSelectQuery(I id) {
        SelectEventRecordsById.Builder<I> builder = SelectEventRecordsById.newBuilder();
        SelectEventRecordsById<I> query = builder.setTableName(name())
                                                 .setDataSource(dataSource())
                                                 .setIdColumn(idColumn())
                                                 .setId(id)
                                                 .build();
        return query;
    }

    @Override
    protected InsertAggregateRecordQuery<I> composeInsertQuery(I id, AggregateEventRecord record) {
        InsertAggregateRecordQuery.Builder<I> builder = InsertAggregateRecordQuery.newBuilder();
        InsertAggregateRecordQuery<I> query = builder.setTableName(name())
                                                     .setDataSource(dataSource())
                                                     .setIdColumn(idColumn())
                                                     .setId(id)
                                                     .setRecord(record)
                                                     .build();
        return query;
    }

    SelectVersionBySnapshot<I>
    composeSelectVersionQuery(int snapshotIndex, @Nullable Timestamp date) {
        SelectVersionBySnapshot.Builder<I> builder = SelectVersionBySnapshot.newBuilder();
        SelectVersionBySnapshot<I> query = builder.setTableName(name())
                                                  .setDataSource(dataSource())
                                                  .setIdColumn(idColumn())
                                                  .setSnapshotIndex(snapshotIndex)
                                                  .setDate(date)
                                                  .build();
        return query;
    }

    private DeletePriorRecordsQuery<I> composeDeleteQuery(I id, Integer version) {
        DeletePriorRecordsQuery.Builder<I> builder = DeletePriorRecordsQuery.newBuilder();
        DeletePriorRecordsQuery<I> query = builder.setTableName(name())
                                                  .setDataSource(dataSource())
                                                  .setIdColumn(idColumn())
                                                  .setId(id)
                                                  .setVersion(version)
                                                  .build();
        return query;
    }

    /**
     * Always throws an {@link IllegalStateException}, because {@link AggregateEventRecord}
     * is immutable.
     *
     * @throws IllegalStateException
     *         always
     */
    @Override
    protected WriteQuery composeUpdateQuery(I id, AggregateEventRecord record) {
        String errMsg = "AggregateEventRecord is immutable and should not be updated.";
        throw newIllegalStateException(errMsg);
    }

    /**
     * The enumeration of the columns of an {@link AggregateEventRecordTable}.
     */
    enum Column implements TableColumn {

        ID,
        AGGREGATE(BYTE_ARRAY),

        /**
         * The {@linkplain AggregateEventRecord.KindCase kind} of an aggregate record
         * in a {@linkplain AggregateEventRecord.KindCase#toString() string} representation.
         */
        KIND(STRING_255),
        TIMESTAMP(LONG),
        TIMESTAMP_NANOS(INT),
        VERSION(INT);

        private final Type type;

        Column(Type type) {
            this.type = type;
        }

        /**
         * Creates a column, {@linkplain #type() type} of which is unknown at the compile time.
         */
        Column() {
            this.type = null;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public boolean isPrimaryKey() {
            return false;
        }

        @Override
        public boolean isNullable() {
            return false;
        }
    }
}
