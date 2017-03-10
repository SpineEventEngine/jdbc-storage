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

package org.spine3.server.storage.jdbc.table.entity.aggregate;

import com.google.protobuf.Int32Value;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.aggregate.query.EventCountQueryFactory;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.table.TableColumn;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import static org.spine3.server.storage.jdbc.Sql.Type.BIGINT;
import static org.spine3.server.storage.jdbc.Sql.Type.ID;
import static org.spine3.server.storage.jdbc.util.DbTableNameFactory.newTableName;

/**
 * A table for storing the event count after the last snapshot.
 *
 * <p>Used in the {@link org.spine3.server.storage.jdbc.aggregate.JdbcAggregateStorage}.
 *
 * @author Dmytro Dashenkov
 */
public class EventCountTable<I> extends AggregateTable<I, Int32Value, EventCountTable.Column> {

    private static final String TABLE_NAME_POSTFIX = "event_count";

    private final EventCountQueryFactory<I> queryFactory;

    public EventCountTable(Class<? extends Entity<I, ?>> entityClass,
                              DataSourceWrapper dataSource) {
        super(newTableName(entityClass) + (TABLE_NAME_POSTFIX),
              entityClass,
              Column.id.name(),
              dataSource);
        this.queryFactory = new EventCountQueryFactory<>(getDataSource(),
                                                         getName(),
                                                         getIdColumn(),
                                                         log());
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    @Override
    protected QueryFactory<I, Int32Value> getQueryFactory() {
        return queryFactory;
    }

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
    }
}
