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

package io.spine.server.storage.jdbc.aggregate.query;

import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.LifecycleFlagField;
import io.spine.server.storage.jdbc.entity.lifecycleflags.query.InsertAndMarkEntityQuery;
import io.spine.server.storage.jdbc.entity.lifecycleflags.query.InsertLifecycleFlagsQuery;
import io.spine.server.storage.jdbc.entity.lifecycleflags.query.MarkEntityQuery;
import io.spine.server.storage.jdbc.entity.lifecycleflags.query.SelectLifecycleFlagsQuery;
import io.spine.server.storage.jdbc.entity.lifecycleflags.query.UpdateLifecycleFlagsQuery;
import io.spine.server.storage.jdbc.query.QueryFactory;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.table.entity.aggregate.LifecycleFlagsTable;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;
import org.slf4j.Logger;

import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;

/**
 * An implementation of the {@link QueryFactory} for generating queries for
 * the {@link LifecycleFlagsTable}.
 *
 * @author Dmytro Dashenkov
 */
public class LifecycleFlagsQueryFactory<I> implements QueryFactory<I, LifecycleFlags> {

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
    public SelectByIdQuery<I, LifecycleFlags> newSelectByIdQuery(I id) {
        final SelectByIdQuery<I, LifecycleFlags> query =
                SelectLifecycleFlagsQuery.<I>newBuilder(tableName)
                        .setDataSource(dataSource)
                        .setLogger(logger)
                        .setIdColumn(idColumn)
                        .setId(id)
                        .build();
        return query;
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

    public MarkEntityQuery<I> newMarkArchivedQuery(I id) {
        return newMarkQuery(id, archived);
    }

    public MarkEntityQuery<I> newMarkDeletedQuery(I id) {
        return newMarkQuery(id, deleted);
    }

    public InsertAndMarkEntityQuery<I> newMarkArchivedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, archived);
    }

    public InsertAndMarkEntityQuery<I> newMarkDeletedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, deleted);
    }

    private InsertAndMarkEntityQuery<I> newInsertAndMarkEntityQuery(I id,
                                                                    LifecycleFlagField column) {
        final InsertAndMarkEntityQuery<I> query =
                InsertAndMarkEntityQuery.<I>newInsertBuilder()
                        .setDataSource(dataSource)
                        .setTableName(tableName)
                        .setLogger(logger)
                        .setColumn(column)
                        .setIdColumn(idColumn)
                        .setId(id)
                        .build();
        return query;
    }

    private MarkEntityQuery<I> newMarkQuery(I id, LifecycleFlagField column) {
        final MarkEntityQuery<I> query =
                MarkEntityQuery.<I>newBuilder()
                        .setDataSource(dataSource)
                        .setLogger(logger)
                        .setTableName(tableName)
                        .setColumn(column)
                        .setIdColumn(idColumn)
                        .setId(id)
                        .build();
        return query;
    }
}
