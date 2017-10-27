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

import com.google.protobuf.Int32Value;
import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.EventCountTable;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import org.slf4j.Logger;

import java.util.Iterator;

/**
 * An implementation of the query factory generating queries for the {@link EventCountTable}.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public class EventCountQueryFactory<I> implements ReadQueryFactory<I, Int32Value>,
                                                  WriteQueryFactory<I, Int32Value>{

    private final String tableName;
    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;

    public EventCountQueryFactory(DataSourceWrapper dataSource,
                                  String tableName,
                                  IdColumn<I> idColumn,
                                  Logger logger) {
        this.tableName = tableName;
        this.idColumn = idColumn;
        this.dataSource = dataSource;
    }

    @Override
    public SelectByIdQuery<I, Int32Value> newSelectByIdQuery(I id) {
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.newBuilder();
        final SelectEventCountByIdQuery<I> query = builder.setTableName(tableName)
                                                          .setDataSource(dataSource)
                                                          .setId(id)
                                                          .setIdColumn(idColumn)
                                                          .build();
        return query;
    }

    @Override
    public SelectQuery<Iterator<I>> newIndexQuery() {
        final StorageIndexQuery.Builder<I> builder = StorageIndexQuery.newBuilder();
        return builder.setDataSource(dataSource)
                      .setTableName(tableName)
                      .setIdColumn(idColumn)
                      .build();
    }

    @Override
    public WriteQuery newInsertQuery(I id, Int32Value record) {
        final InsertEventCountQuery.Builder<I> builder = InsertEventCountQuery.newBuilder();
        final WriteQuery query = builder.setTableName(tableName)
                                        .setId(id)
                                        .setIdColumn(idColumn)
                                        .setDataSource(dataSource)
                                        .setEventCount(record.getValue())
                                        .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(I id, Int32Value record) {
        final UpdateEventCountQuery.Builder<I> builder = UpdateEventCountQuery.newBuilder();
        final WriteQuery query = builder.setDataSource(dataSource)
                                        .setTableName(tableName)
                                        .setId(id)
                                        .setIdColumn(idColumn)
                                        .setEventCount(record.getValue())
                                        .build();
        return query;
    }
}
