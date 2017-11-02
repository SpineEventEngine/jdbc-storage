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
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;

import java.util.List;

import static io.spine.server.storage.jdbc.Sql.Type.ID;
import static io.spine.server.storage.jdbc.Sql.Type.INT;
import static io.spine.server.storage.jdbc.aggregate.EventCountTable.Column.id;

/**
 * A table for storing the event count after the last snapshot.
 *
 * <p>Used in the {@link JdbcAggregateStorage}.
 *
 * @author Dmytro Dashenkov
 */
class EventCountTable<I> extends AggregateTable<I, Integer, Integer> {

    private static final String TABLE_NAME_POSTFIX = "_event_count";

    private final EventCountWriteQueryFactory<I> writeQueryFactory;

    EventCountTable(Class<? extends Entity<I, ?>> entityClass,
                    DataSourceWrapper dataSource) {
        super(TABLE_NAME_POSTFIX, entityClass, id.name(), dataSource);
        this.writeQueryFactory = new EventCountWriteQueryFactory<>(getIdColumn(),
                                                                   getDataSource(),
                                                                   getName());
    }

    @Override
    protected Column getIdColumnDeclaration() {
        return id;
    }

    @Override
    protected List<? extends TableColumn> getTableColumns() {
        return ImmutableList.copyOf(Column.values());
    }


    @Override
    protected WriteQueryFactory<I, Integer> getWriteQueryFactory() {
        return writeQueryFactory;
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

    /**
     * The enumeration of the columns of an {@link EventCountTable}.
     */
    enum Column implements TableColumn {

        id(ID),
        event_count(INT);

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
            return this == id;
        }

        @Override
        public boolean isNullable() {
            return false;
        }
    }
}
