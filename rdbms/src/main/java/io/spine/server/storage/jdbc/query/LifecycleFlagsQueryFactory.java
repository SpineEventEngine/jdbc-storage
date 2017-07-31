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

package io.spine.server.storage.jdbc.query;

import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.LifecycleFlagsTable;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import org.slf4j.Logger;

/**
 * An implementation of the query factory for generating queries for
 * the {@link LifecycleFlagsTable}.
 *
 * @author Dmytro Dashenkov
 */
public class LifecycleFlagsQueryFactory<I> implements ReadQueryFactory<I, LifecycleFlags>,
                                                      WriteQueryFactory<I, LifecycleFlags> {

    private final Logger logger;
    private final DataSourceWrapper dataSource;
    private final IdColumn<I> idColumn;
    private final String tableName;

    public LifecycleFlagsQueryFactory(DataSourceWrapper dataSource,
                                      Logger logger,
                                      IdColumn<I> idColumn,
                                      String tableName) {
        this.logger = logger;
        this.dataSource = dataSource;
        this.idColumn = idColumn;
        this.tableName = tableName;
    }

    @Override
    public SelectMessageByIdQuery<I, LifecycleFlags> newSelectByIdQuery(I id) {
        final SelectMessageByIdQuery<I, LifecycleFlags> query =
                SelectLifecycleFlagsQuery.<I>newBuilder(tableName)
                                         .setDataSource(dataSource)
                                         .setLogger(logger)
                                         .setIdColumn(idColumn)
                                         .setId(id)
                                         .build();
        return query;
    }

    @Override
    public StorageIndexQuery<I> newIndexQuery() {
        return StorageIndexQuery.<I>newBuilder()
                                .setDataSource(dataSource)
                                .setLogger(logger)
                                .setTableName(tableName)
                                .setIdType(idColumn.getJavaType())
                                .setIdColumnName(idColumn.getColumnName())
                                .build();
    }

    @Override
    public WriteQuery newInsertQuery(I id, LifecycleFlags record) {
        final WriteQuery query = InsertLifecycleFlagsQuery.<I>newBuilder(tableName)
                                                          .setId(id)
                                                          .setLifecycleFlags(record)
                                                          .setLogger(logger)
                                                          .setDataSource(dataSource)
                                                          .setIdColumn(idColumn)
                                                          .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(I id, LifecycleFlags record) {
        final WriteQuery query = UpdateLifecycleFlagsQuery.<I>newBuilder(tableName)
                                                          .setLogger(logger)
                                                          .setDataSource(dataSource)
                                                          .setId(id)
                                                          .setLifecycleFlags(record)
                                                          .setIdColumn(idColumn)
                                                          .build();
        return query;
    }
}
