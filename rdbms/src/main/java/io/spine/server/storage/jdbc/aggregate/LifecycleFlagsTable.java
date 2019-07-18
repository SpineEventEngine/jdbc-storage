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

import com.google.common.collect.ImmutableList;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.ID;

/**
 * A table for storing the {@link LifecycleFlags} of an {@link Aggregate}.
 */
class LifecycleFlagsTable<I> extends EntityTable<I, LifecycleFlags, LifecycleFlags> {

    private static final String TABLE_NAME_POSTFIX = "_visibility";

    LifecycleFlagsTable(Class<? extends Aggregate<I, ?, ?>> aggregateClass,
                        DataSourceWrapper dataSource,
                        TypeMapping typeMapping) {
        super(TABLE_NAME_POSTFIX, aggregateClass, ID.name(), dataSource, typeMapping);
    }

    @Override
    protected Column idColumnDeclaration() {
        return ID;
    }

    @Override
    protected List<? extends TableColumn> tableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    @Override
    protected SelectQuery<LifecycleFlags> composeSelectQuery(I id) {
        SelectLifecycleFlagsQuery.Builder<I> builder = SelectLifecycleFlagsQuery.newBuilder();
        SelectQuery<LifecycleFlags> query = builder.setTableName(getName())
                                                   .setDataSource(getDataSource())
                                                   .setIdColumn(getIdColumn())
                                                   .setId(id)
                                                   .build();
        return query;
    }

    @Override
    protected WriteQuery composeInsertQuery(I id, LifecycleFlags record) {
        InsertLifecycleFlagsQuery.Builder<I> builder = InsertLifecycleFlagsQuery.newBuilder();
        WriteQuery query = builder.setTableName(getName())
                                  .setId(id)
                                  .setLifecycleFlags(record)
                                  .setDataSource(getDataSource())
                                  .setIdColumn(getIdColumn())
                                  .build();
        return query;
    }

    @Override
    protected WriteQuery composeUpdateQuery(I id, LifecycleFlags record) {
        UpdateLifecycleFlagsQuery.Builder<I> builder = UpdateLifecycleFlagsQuery.newBuilder();
        WriteQuery query = builder.setTableName(getName())
                                  .setDataSource(getDataSource())
                                  .setId(id)
                                  .setLifecycleFlags(record)
                                  .setIdColumn(getIdColumn())
                                  .build();
        return query;
    }

    /**
     * The enumeration of the columns of a {@link LifecycleFlagsTable}.
     */
    enum Column implements TableColumn {

        ID,
        ARCHIVED(BOOLEAN),
        DELETED(BOOLEAN);

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
