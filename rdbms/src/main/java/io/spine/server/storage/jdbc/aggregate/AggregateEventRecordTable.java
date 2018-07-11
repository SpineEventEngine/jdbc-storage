/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A table for storing the {@linkplain AggregateEventRecord aggregate event records}.
 *
 * @author Dmytro Dashenkov
 */
class AggregateEventRecordTable<I> extends EntityTable<I,
                                                       DbIterator<AggregateEventRecord>,
                                                       AggregateEventRecord> {

    AggregateEventRecordTable(Class<? extends Aggregate<I, ?, ?>> entityClass,
                              DataSourceWrapper dataSource,
                              TypeMapping typeMapping) {
        super(entityClass, Column.ID.name(), dataSource, typeMapping);
    }

    @Override
    protected Column getIdColumnDeclaration() {
        return Column.ID;
    }

    @Override
    protected List<? extends TableColumn> getTableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    @Override
    protected SelectEventRecordsById<I> composeSelectQuery(I id) {
        SelectEventRecordsById.Builder<I> builder = SelectEventRecordsById.newBuilder();
        SelectEventRecordsById<I> query = builder.setTableName(getName())
                                                 .setDataSource(getDataSource())
                                                 .setIdColumn(getIdColumn())
                                                 .setId(id)
                                                 .build();
        return query;
    }

    @Override
    protected InsertAggregateRecordQuery<I> composeInsertQuery(I id, AggregateEventRecord record) {
        InsertAggregateRecordQuery.Builder<I> builder = InsertAggregateRecordQuery.newBuilder();
        InsertAggregateRecordQuery<I> query = builder.setTableName(getName())
                                                     .setDataSource(getDataSource())
                                                     .setIdColumn(getIdColumn())
                                                     .setId(id)
                                                     .setRecord(record)
                                                     .build();
        return query;
    }

    /**
     * @throws IllegalStateException always, because {@link AggregateEventRecord} is immutable
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
