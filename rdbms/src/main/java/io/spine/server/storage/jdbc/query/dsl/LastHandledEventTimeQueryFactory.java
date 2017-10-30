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

import com.google.protobuf.Timestamp;
import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.LastHandledEventTimeTable;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;

import static io.spine.server.storage.jdbc.IdColumn.typeString;
import static io.spine.server.storage.jdbc.LastHandledEventTimeTable.Column.projection_type;

/**
 * The query factory for interaction with the {@link LastHandledEventTimeTable}.
 *
 * @author Andrey Lavrov
 */
@Internal
public class LastHandledEventTimeQueryFactory extends AbstractReadQueryFactory<String, Timestamp>
        implements WriteQueryFactory<String, Timestamp> {

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     */
    public LastHandledEventTimeQueryFactory(DataSourceWrapper dataSource, String tableName) {
        super(typeString(projection_type.name()), dataSource, tableName);
    }

    @Override
    public SelectByIdQuery<String, Timestamp> newSelectByIdQuery(String id) {
        final SelectTimestampQuery.Builder builder = SelectTimestampQuery.newBuilder();
        final SelectTimestampQuery query = builder.setTableName(getTableName())
                                                  .setDataSource(getDataSource())
                                                  .setId(id)
                                                  .setIdColumn(getIdColumn())
                                                  .build();
        return query;
    }

    @Override
    public WriteQuery newInsertQuery(String id, Timestamp record) {
        final InsertTimestampQuery.Builder builder = InsertTimestampQuery.newBuilder();
        final InsertTimestampQuery query = builder.setTableName(getTableName())
                                                  .setDataSource(getDataSource())
                                                  .setId(id)
                                                  .setTimestamp(record)
                                                  .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(String id, Timestamp record) {
        final UpdateTimestampQuery.Builder builder = UpdateTimestampQuery.newBuilder();
        final UpdateTimestampQuery query = builder.setTableName(getTableName())
                                                  .setDataSource(getDataSource())
                                                  .setId(id)
                                                  .setTimestamp(record)
                                                  .build();
        return query;
    }
}
