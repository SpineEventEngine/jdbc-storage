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
import io.spine.server.storage.jdbc.query.WriteQuery;

import static io.spine.server.storage.jdbc.IdColumn.typeString;
import static io.spine.server.storage.jdbc.LastHandledEventTimeTable.Column.projection_type;

/**
 * An implementation of the query factory for generating write queries for
 * the {@link io.spine.server.storage.jdbc.LastHandledEventTimeTable LastHandledEventTimeTable}.
 *
 * @author Dmytro Grankin
 */
@Internal
public class LastHandledEventTimeWriteFactory extends AbstractWriteQueryFactory<String, Timestamp> {

    public LastHandledEventTimeWriteFactory(DataSourceWrapper dataSource, String tableName) {
        super(typeString(projection_type.name()), dataSource, tableName);
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
