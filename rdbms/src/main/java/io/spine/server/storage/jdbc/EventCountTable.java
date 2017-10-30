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

package io.spine.server.storage.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Int32Value;
import io.spine.annotation.Internal;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.query.EventCountReadQueryFactory;
import io.spine.server.storage.jdbc.query.EventCountWriteQueryFactory;

import java.util.List;

import static io.spine.server.storage.jdbc.DbTableNameFactory.newTableName;
import static io.spine.server.storage.jdbc.Sql.Type.BIGINT;
import static io.spine.server.storage.jdbc.Sql.Type.ID;

/**
 * A table for storing the event count after the last snapshot.
 *
 * <p>Used in the {@link JdbcAggregateStorage}.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public class EventCountTable<I> extends AggregateTable<I, Int32Value> {

    private static final String TABLE_NAME_POSTFIX = "_event_count";

    private final EventCountReadQueryFactory<I> readQueryFactory;
    private final EventCountWriteQueryFactory<I> writeQueryFactory;

    EventCountTable(Class<? extends Entity<I, ?>> entityClass,
                    DataSourceWrapper dataSource) {
        super(newTableName(entityClass) + (TABLE_NAME_POSTFIX),
              entityClass,
              Column.id.name(),
              dataSource);
        this.readQueryFactory = new EventCountReadQueryFactory<>(getIdColumn(),
                                                                 getDataSource(),
                                                                 getName());
        this.writeQueryFactory = new EventCountWriteQueryFactory<>(getIdColumn(),
                                                                   getDataSource(),
                                                                   getName());
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
    protected ReadQueryFactory<I, Int32Value> getReadQueryFactory() {
        return readQueryFactory;
    }

    @Override
    protected WriteQueryFactory<I, Int32Value> getWriteQueryFactory() {
        return writeQueryFactory;
    }

    /**
     * The enumeration of the columns of an {@link EventCountTable}.
     */
    @Internal
    public enum Column implements TableColumn {

        id(ID),
        event_count(BIGINT);

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
