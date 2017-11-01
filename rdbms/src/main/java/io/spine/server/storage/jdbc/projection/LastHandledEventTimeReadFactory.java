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

package io.spine.server.storage.jdbc.projection;

import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.AbstractReadQueryFactory;
import io.spine.server.storage.jdbc.query.SelectQuery;

import static io.spine.server.storage.jdbc.IdColumn.typeString;
import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.projection_type;

/**
 * An implementation of the query factory for generating read queries for
 * the {@link LastHandledEventTimeTable}.
 *
 * @author Andrey Lavrov
 */
class LastHandledEventTimeReadFactory extends AbstractReadQueryFactory<String, Timestamp> {

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     */
    LastHandledEventTimeReadFactory(DataSourceWrapper dataSource, String tableName) {
        super(typeString(projection_type.name()), dataSource, tableName);
    }

    @Override
    public SelectQuery<Timestamp> newSelectByIdQuery(String id) {
        final SelectTimestampQuery.Builder builder = SelectTimestampQuery.newBuilder();
        final SelectTimestampQuery query = builder.setTableName(getTableName())
                                                  .setDataSource(getDataSource())
                                                  .setId(id)
                                                  .setIdColumn(getIdColumn())
                                                  .build();
        return query;
    }
}
