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

import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.table.TableColumn;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.DbIterator;

import static io.spine.server.storage.jdbc.Sql.Type.BIGINT;
import static io.spine.server.storage.jdbc.Sql.Type.BLOB;
import static io.spine.server.storage.jdbc.Sql.Type.INT;
import static io.spine.server.storage.jdbc.Sql.Type.ID;

/**
 * A table for storing the {@linkplain AggregateEventRecord aggregate event records}.
 *
 * @author Dmytro Dashenkov
 */
public class AggregateEventRecordTable<I>
        extends AggregateTable<I, AggregateEventRecord, AggregateEventRecordTable.Column> {

    private final AggregateStorageQueryFactory<I> queryFactory;

    public AggregateEventRecordTable(Class<? extends Aggregate<I, ?, ?>> entityClass,
                                     DataSourceWrapper dataSource,
                                     ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry) {
        super(entityClass, Column.id.name(), dataSource, columnTypeRegistry);
        queryFactory = new AggregateStorageQueryFactory<>(dataSource, entityClass, getIdColumn());
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
    protected ReadQueryFactory<I, AggregateEventRecord> getReadQueryFactory() {
        return queryFactory;
    }

    @Override
    protected WriteQueryFactory<I, AggregateEventRecord> getWriteQueryFactory() {
        return null;
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

    public enum Column implements TableColumn {

        id(ID),
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
