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

package io.spine.server.storage.jdbc.query.dsl;

import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.LifecycleFlagsTable;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import org.slf4j.Logger;

/**
 * An implementation of the query factory for generating queries for
 * the {@link LifecycleFlagsTable}.
 *
 * @author Dmytro Dashenkov
 */
public class LifecycleFlagsQueryFactory<I> extends AbstractReadQueryFactory<I, LifecycleFlags>
        implements WriteQueryFactory<I, LifecycleFlags> {

    public LifecycleFlagsQueryFactory(DataSourceWrapper dataSource,
                                      Logger logger,
                                      IdColumn<I> idColumn,
                                      String tableName) {
        super(idColumn, dataSource, tableName);
    }

    @Override
    public SelectByIdQuery<I, LifecycleFlags> newSelectByIdQuery(I id) {
        final SelectLifecycleFlagsQuery.Builder<I> builder = SelectLifecycleFlagsQuery.newBuilder();
        final SelectByIdQuery<I, LifecycleFlags> query = builder.setTableName(getTableName())
                                                                .setDataSource(getDataSource())
                                                                .setIdColumn(getIdColumn())
                                                                .setId(id)
                                                                .build();
        return query;
    }

    @Override
    public WriteQuery newInsertQuery(I id, LifecycleFlags record) {
        final InsertLifecycleFlagsQuery.Builder<I> builder = InsertLifecycleFlagsQuery.newBuilder();
        final WriteQuery query = builder.setTableName(getTableName())
                                        .setId(id)
                                        .setLifecycleFlags(record)
                                        .setDataSource(getDataSource())
                                        .setIdColumn(getIdColumn())
                                        .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(I id, LifecycleFlags record) {
        final UpdateLifecycleFlagsQuery.Builder<I> builder = UpdateLifecycleFlagsQuery.newBuilder();
        final WriteQuery query = builder.setTableName(getTableName())
                                        .setDataSource(getDataSource())
                                        .setId(id)
                                        .setLifecycleFlags(record)
                                        .setIdColumn(getIdColumn())
                                        .build();
        return query;
    }
}
