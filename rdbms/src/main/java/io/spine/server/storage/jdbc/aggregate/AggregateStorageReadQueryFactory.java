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

package io.spine.server.storage.jdbc.aggregate;

import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.AbstractReadQueryFactory;

/**
 * The {@code ReadQueryFactory} for {@link AggregateEventRecordTable}.
 *
 * @param <I> the type of IDs used in the storage
 * @author Dmytro Grankin
 */
class AggregateStorageReadQueryFactory<I>
        extends AbstractReadQueryFactory<I, DbIterator<AggregateEventRecord>> {

    AggregateStorageReadQueryFactory(IdColumn<I> idColumn,
                                     DataSourceWrapper dataSource,
                                     String tableName) {
        super(idColumn, dataSource, tableName);
    }

    /**
     * Creates the {@linkplain SelectEventRecordsById query} that selects
     * aggregate records by an aggregate ID.
     *
     * @param id the aggregate ID
     * @return the select query for {@linkplain AggregateEventRecord aggregate event records}
     */
    @Override
    public SelectEventRecordsById<I> newSelectByIdQuery(I id) {
        final SelectEventRecordsById.Builder<I> builder = SelectEventRecordsById.newBuilder();
        return builder.setTableName(getTableName())
                      .setDataSource(getDataSource())
                      .setIdColumn(getIdColumn())
                      .setId(id)
                      .build();
    }
}
