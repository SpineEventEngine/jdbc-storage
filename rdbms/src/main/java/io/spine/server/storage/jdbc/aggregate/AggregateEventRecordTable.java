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
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;

import java.util.List;

import static io.spine.server.storage.jdbc.Sql.Type.BIGINT;
import static io.spine.server.storage.jdbc.Sql.Type.BLOB;
import static io.spine.server.storage.jdbc.Sql.Type.ID;
import static io.spine.server.storage.jdbc.Sql.Type.INT;

/**
 * A table for storing the {@linkplain AggregateEventRecord aggregate event records}.
 *
 * @author Dmytro Dashenkov
 */
class AggregateEventRecordTable<I>
        extends AggregateTable<I, DbIterator<AggregateEventRecord>, AggregateEventRecord> {

    private final AggregateStorageWriteQueryFactory<I> writeQueryFactory;

    AggregateEventRecordTable(Class<? extends Aggregate<I, ?, ?>> entityClass,
                              DataSourceWrapper dataSource) {
        super(entityClass, Column.id.name(), dataSource);
        writeQueryFactory = new AggregateStorageWriteQueryFactory<>(getIdColumn(),
                                                                    dataSource,
                                                                    getName());
    }

    @Override
    protected Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected WriteQueryFactory<I, AggregateEventRecord> getWriteQueryFactory() {
        return writeQueryFactory;
    }

    @Override
    protected List<? extends TableColumn> getTableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // No extra presence checks are required
    @Override
    public void write(I id, AggregateEventRecord record) {
        writeQueryFactory.newInsertQuery(id, record)
                         .execute();
    }

    @Override
    protected SelectQuery<DbIterator<AggregateEventRecord>> composeSelectQuery(I id) {
        final SelectEventRecordsById.Builder<I> builder = SelectEventRecordsById.newBuilder();
        return builder.setTableName(getName())
                      .setDataSource(getDataSource())
                      .setIdColumn(getIdColumn())
                      .setId(id)
                      .build();
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
