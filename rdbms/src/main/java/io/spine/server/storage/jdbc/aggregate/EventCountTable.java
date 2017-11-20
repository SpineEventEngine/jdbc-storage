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
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.aggregate.EventCountTable.Column.ID;

/**
 * A table for storing the
 * {@link io.spine.server.aggregate.AggregateStorage#readEventCountAfterLastSnapshot(Object)
 * event count after the last snapshot}.
 *
 * <p>This table exists for the performance reasons. It acts as a cache for data
 * that is used in the command handling lifecycle.
 *
 * <p>The event count after the snapshot can be retrieved from {@link AggregateEventRecordTable}
 * using a query, but this approach is less efficient for a majority of RDBMS
 * due to a massive table scanning involved.
 *
 * <p>Used in the {@link JdbcAggregateStorage}.
 *
 * @author Dmytro Dashenkov
 */
class EventCountTable<I> extends EntityTable<I, Integer, Integer> {

    private static final String TABLE_NAME_POSTFIX = "_event_count";

    EventCountTable(Class<? extends Entity<I, ?>> entityClass,
                    DataSourceWrapper dataSource,
                    TypeMapping typeMapping) {
        super(TABLE_NAME_POSTFIX, entityClass, ID.name(), dataSource, typeMapping);
    }

    @Override
    protected Column getIdColumnDeclaration() {
        return ID;
    }

    @Override
    protected List<? extends TableColumn> getTableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    @Override
    protected SelectQuery<Integer> composeSelectQuery(I id) {
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.newBuilder();
        final SelectEventCountByIdQuery<I> query = builder.setTableName(getName())
                                                          .setDataSource(getDataSource())
                                                          .setId(id)
                                                          .setIdColumn(getIdColumn())
                                                          .build();
        return query;
    }

    @Override
    protected WriteQuery composeInsertQuery(I id, Integer record) {
        final InsertEventCountQuery.Builder<I> builder = InsertEventCountQuery.newBuilder();
        final WriteQuery query = builder.setTableName(getName())
                                        .setId(id)
                                        .setIdColumn(getIdColumn())
                                        .setDataSource(getDataSource())
                                        .setEventCount(record)
                                        .build();
        return query;
    }

    @Override
    protected WriteQuery composeUpdateQuery(I id, Integer record) {
        final UpdateEventCountQuery.Builder<I> builder = UpdateEventCountQuery.newBuilder();
        final WriteQuery query = builder.setDataSource(getDataSource())
                                        .setTableName(getName())
                                        .setId(id)
                                        .setIdColumn(getIdColumn())
                                        .setEventCount(record)
                                        .build();
        return query;
    }

    /**
     * The enumeration of the columns of an {@link EventCountTable}.
     */
    enum Column implements TableColumn {

        ID,
        EVENT_COUNT(INT);

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
            return this == ID;
        }

        @Override
        public boolean isNullable() {
            return false;
        }
    }
}
