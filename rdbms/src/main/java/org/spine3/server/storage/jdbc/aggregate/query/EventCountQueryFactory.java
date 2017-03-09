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

package org.spine3.server.storage.jdbc.aggregate.query;

import com.google.protobuf.Int32Value;
import org.slf4j.Logger;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

/**
 * @author Dmytro Dashenkov.
 */
public class EventCountQueryFactory<I> implements QueryFactory<I, Int32Value> {

    private final String tableName;
    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final Logger logger;

    public EventCountQueryFactory(DataSourceWrapper dataSource,
                                  String tableName,
                                  IdColumn<I> idColumn,
                                  Logger logger) {
        this.tableName = tableName;
        this.idColumn = idColumn;
        this.dataSource = dataSource;
        this.logger = logger;
    }

    @Override
    public SelectByIdQuery<I, Int32Value> newSelectByIdQuery(I id) {
        final SelectByIdQuery<I, Int32Value> query =
                SelectEventCountByIdQuery.<I>newBuilder(tableName)
                                         .setDataSource(dataSource)
                                         .setLogger(logger)
                                         .setId(id)
                                         .setIdColumn(idColumn)
                                         .build();
        return query;
    }

    @Override
    public WriteQuery newInsertQuery(I id, Int32Value record) {
        final WriteQuery query = InsertEventCountQuery.<I>newBuilder(tableName)
                                                      .setId(id)
                                                      .setIdColumn(idColumn)
                                                      .setLogger(logger)
                                                      .setDataSource(dataSource)
                                                      .setCount(record.getValue())
                                                      .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(I id, Int32Value record) {
        final WriteQuery query = UpdateEventCountQuery.<I>newBuilder(tableName)
                                                      .setDataSource(dataSource)
                                                      .setLogger(logger)
                                                      .setId(id)
                                                      .setIdColumn(idColumn)
                                                      .setCount(record.getValue())
                                                      .build();
        return query;
    }
}
