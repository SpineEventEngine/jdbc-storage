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

import com.google.common.collect.ImmutableList;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static io.spine.server.storage.jdbc.Sql.Type.BIGINT;
import static io.spine.server.storage.jdbc.Sql.Type.BLOB;
import static io.spine.server.storage.jdbc.Sql.Type.ID;
import static io.spine.server.storage.jdbc.Sql.Type.INT;
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
                              DataSourceWrapper dataSource) {
        super(entityClass, Column.id.name(), dataSource);
    }

    @Override
    protected Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected List<? extends TableColumn> getTableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    @Override
    protected SelectEventRecordsById<I> composeSelectQuery(I id) {
        final SelectEventRecordsById.Builder<I> builder = SelectEventRecordsById.newBuilder();
        return builder.setTableName(getName())
                      .setDataSource(getDataSource())
                      .setIdColumn(getIdColumn())
                      .setId(id)
                      .build();
    }

    @Override
    protected WriteQuery composeInsertQuery(I id, AggregateEventRecord record) {
        final InsertAggregateRecordQuery.Builder<I> builder = InsertAggregateRecordQuery.newBuilder();
        final InsertAggregateRecordQuery<I> query = builder.setTableName(getName())
                                                           .setDataSource(getDataSource())
                                                           .setIdColumn(getIdColumn())
                                                           .setId(id)
                                                           .setRecord(record)
                                                           .build();
        return query;
    }

    /**
     * @throws IllegalStateException always,
     *                               because {@link AggregateEventRecord} should be immutable
     */
    @Override
    protected WriteQuery composeUpdateQuery(I id, AggregateEventRecord record) {
        final String errMsg = "AggregateEventRecord is immutable and should not be updated.";
        throw newIllegalStateException(errMsg);
    }

    /**
     * The enumeration of the columns of an {@link AggregateEventRecordTable}.
     */
    enum Column implements TableColumn {

        id(ID),
        aggregate(BLOB),
        timestamp(BIGINT),
        timestamp_nanos(INT),
        version(INT);

        private final Sql.Type type;

        Column(Sql.Type type) {
            this.type = type;
        }

        @Override
        public Sql.Type type() {
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
