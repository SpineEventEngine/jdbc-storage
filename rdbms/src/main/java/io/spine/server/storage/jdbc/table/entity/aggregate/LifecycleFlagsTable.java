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

package io.spine.server.storage.jdbc.table.entity.aggregate;

import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.aggregate.query.LifecycleFlagsQueryFactory;
import io.spine.server.storage.jdbc.entity.lifecycleflags.query.MarkEntityQuery;
import io.spine.server.storage.jdbc.query.QueryFactory;
import io.spine.server.storage.jdbc.table.TableColumn;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.DbTableNameFactory;

import static io.spine.server.storage.jdbc.Sql.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Sql.Type.ID;

/**
 * A table for storing the {@link LifecycleFlags} of an {@link Aggregate}.
 *
 * @author Dmytro Dashenkov
 */
public class LifecycleFlagsTable<I> extends AggregateTable<I, LifecycleFlags, LifecycleFlagsTable.Column> {

    private static final String TABLE_NAME_POSTFIX = "visibility";

    private final LifecycleFlagsQueryFactory<I> queryFactory;

    public LifecycleFlagsTable(Class<? extends Aggregate<I, ?, ?>> aggregateClass,
                               DataSourceWrapper dataSource) {
        super(DbTableNameFactory.newTableName(aggregateClass) + TABLE_NAME_POSTFIX,
              aggregateClass,
              Column.id.name(),
              dataSource);
        this.queryFactory = new LifecycleFlagsQueryFactory<>(dataSource, log(), getIdColumn(),
                                                             getName());
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    // Storing records under string IDs instead of generic
    @Override
    protected QueryFactory<I, LifecycleFlags> getQueryFactory() {
        return queryFactory;
    }

    public void markArchived(I id) {
        final MarkEntityQuery query;
        if (!containsRecord(id)) {
            query = queryFactory.newMarkArchivedNewEntityQuery(id);
        } else {
            query = queryFactory.newMarkArchivedQuery(id);
        }
        query.execute();
    }

    public void markDeleted(I id) {
        final MarkEntityQuery query;
        if (!containsRecord(id)) {
            query = queryFactory.newMarkDeletedNewEntityQuery(id);
        } else {
            query = queryFactory.newMarkDeletedQuery(id);
        }
        query.execute();
    }

    public enum Column implements TableColumn {

        id(ID),
        archived(BOOLEAN),
        deleted(BOOLEAN);

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
