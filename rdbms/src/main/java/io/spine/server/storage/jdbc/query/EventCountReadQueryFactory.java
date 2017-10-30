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

import com.google.protobuf.Int32Value;
import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;

/**
 * An implementation of the query factory generating read queries for the
 * {@link io.spine.server.storage.jdbc.EventCountTable EventCountTable}.
 *
 * @author Dmytro Grankin
 */
@Internal
public class EventCountReadQueryFactory<I> extends AbstractReadQueryFactory<I, Int32Value> {

    public EventCountReadQueryFactory(IdColumn<I> idColumn,
                                      DataSourceWrapper dataSource,
                                      String tableName) {
        super(idColumn, dataSource, tableName);
    }

    @Override
    public SelectByIdQuery<I, Int32Value> newSelectByIdQuery(I id) {
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.newBuilder();
        final SelectEventCountByIdQuery<I> query = builder.setTableName(getTableName())
                                                          .setDataSource(getDataSource())
                                                          .setId(id)
                                                          .setIdColumn(getIdColumn())
                                                          .build();
        return query;
    }
}
