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

import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.AbstractWriteQueryFactory;
import io.spine.server.storage.jdbc.query.WriteQuery;

/**
 * An implementation of the query factory generating write queries for the {@link EventCountTable}.
 *
 * @author Dmytro Dashenkov
 */
class EventCountWriteQueryFactory<I> extends AbstractWriteQueryFactory<I, Integer> {

    EventCountWriteQueryFactory(IdColumn<I> idColumn,
                                DataSourceWrapper dataSource,
                                String tableName) {
        super(idColumn, dataSource, tableName);
    }

    @Override
    public WriteQuery newInsertQuery(I id, Integer record) {
        final InsertEventCountQuery.Builder<I> builder = InsertEventCountQuery.newBuilder();
        final WriteQuery query = builder.setTableName(getTableName())
                                        .setId(id)
                                        .setIdColumn(getIdColumn())
                                        .setDataSource(getDataSource())
                                        .setEventCount(record)
                                        .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(I id, Integer record) {
        final UpdateEventCountQuery.Builder<I> builder = UpdateEventCountQuery.newBuilder();
        final WriteQuery query = builder.setDataSource(getDataSource())
                                        .setTableName(getTableName())
                                        .setId(id)
                                        .setIdColumn(getIdColumn())
                                        .setEventCount(record)
                                        .build();
        return query;
    }
}
