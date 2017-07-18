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

package io.spine.server.storage.jdbc.projection.query;

import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;
import io.spine.server.storage.jdbc.query.StorageIndexQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.table.LastHandledEventTimeTable;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class creates queries for interaction with
 * {@link LastHandledEventTimeTable}.
 *
 * @author Andrey Lavrov
 */
public class LastHandledEventTimeQueryFactory
        implements ReadQueryFactory<String, Timestamp>,
                   WriteQueryFactory<String, Timestamp>{

    private final DataSourceWrapper dataSource;
    private final String tableName;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource      instance of {@link DataSourceWrapper}
     */
    public LastHandledEventTimeQueryFactory(DataSourceWrapper dataSource, String tableName) {
        this.dataSource = dataSource;
        this.tableName = tableName;
    }

    @Override
    public SelectMessageByIdQuery<String, Timestamp> newSelectByIdQuery(String id) {
        final SelectTimestampQuery.Builder builder =
                SelectTimestampQuery.newBuilder(tableName)
                                    .setDataSource(dataSource)
                                    .setId(id)
                                    .setIdColumn(IdColumn.typeString(
                                            LastHandledEventTimeTable.Column.projection_type.name()))
                                    .setLogger(logger);
        return builder.build();
    }

    @Override
    public StorageIndexQuery<String> newIndexQuery() {
        return null;
    }

    @Override
    public WriteQuery newInsertQuery(String id, Timestamp record) {
        final InsertTimestampQuery.Builder builder = InsertTimestampQuery.newBuilder(tableName)
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger)
                                                                         .setId(id)
                                                                         .setTimestamp(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(String id, Timestamp record) {
        final UpdateTimestampQuery.Builder builder = UpdateTimestampQuery.newBuilder(tableName)
                                                                         .setDataSource(dataSource)
                                                                         .setId(id)
                                                                         .setLogger(logger)
                                                                         .setTimestamp(record);
        return builder.build();
    }

    public void setLogger(Logger logger) {
        this.logger = checkNotNull(logger);
    }
}
