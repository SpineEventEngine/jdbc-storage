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

import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateEventRecord;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.table.TableColumn;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import static org.spine3.server.storage.jdbc.Sql.Type.BIGINT;
import static org.spine3.server.storage.jdbc.Sql.Type.BLOB;
import static org.spine3.server.storage.jdbc.Sql.Type.INT;
import static org.spine3.server.storage.jdbc.Sql.Type.UNKNOWN;

/**
 * @author Dmytro Dashenkov.
 */
public class AggregateEventRecordTable<I>
        extends AggregateTable<I, AggregateEventRecord, AggregateEventRecordTable.Column> {

    private final AggregateStorageQueryFactory<I> queryFactory;

    public AggregateEventRecordTable(Class<? extends Aggregate<I, ?, ?>> entityClass,
                                     DataSourceWrapper dataSource) {
        super(entityClass, dataSource);
        queryFactory = new AggregateStorageQueryFactory<>(dataSource, entityClass);
        queryFactory.setLogger(log());
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
    protected QueryFactory<I, AggregateEventRecord> getQueryFactory() {
        return queryFactory;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // No extra presence checks are required
    @Override
    public void write(I id, AggregateEventRecord record) {
        queryFactory.newInsertQuery(id, record)
                    .execute();
    }

    public DbIterator<AggregateEventRecord> historyBackward(I id) {
        final DbIterator<AggregateEventRecord> result =
                queryFactory.newSelectByIdSortedByTimeDescQuery(id)
                            .execute();
        return result;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code false} for the {@code AggregateEventRecordTable}.
     */
    @SuppressWarnings("MethodDoesntCallSuperMethod") // Flag method
    @Override
    protected boolean idIsPrimaryKey() {
        return false;
    }

    enum Column implements TableColumn {

        id(UNKNOWN),
        aggregate(BLOB),
        timestamp(BIGINT),
        timestamp_nanos(INT);

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