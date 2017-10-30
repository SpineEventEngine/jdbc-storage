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

import io.spine.annotation.Internal;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.WriteQuery;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The {@code WriteQueryFactory} for
 * {@link io.spine.server.storage.jdbc.AggregateEventRecordTable AggregateEventRecordTable}.
 *
 * @param <I> the type of IDs used in the storage
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
@Internal
public class AggregateStorageWriteFactory<I> extends AbstractWriteQueryFactory<I, AggregateEventRecord> {

    private final Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     */
    public AggregateStorageWriteFactory(IdColumn<I> idColumn,
                                        DataSourceWrapper dataSource,
                                        String tableName,
                                        Logger logger) {
        super(idColumn, dataSource, tableName);
        this.logger = checkNotNull(logger);
    }

    @Override
    public WriteQuery newInsertQuery(I id, AggregateEventRecord record) {
        final InsertAggregateRecordQuery.Builder<I> builder = InsertAggregateRecordQuery.newBuilder();
        return builder.setTableName(getTableName())
                      .setDataSource(getDataSource())
                      .setIdColumn(getIdColumn())
                      .setId(id)
                      .setRecord(record)
                      .build();
    }

    /**
     * Generates new {@code INSERT} query.
     *
     * <p>{@linkplain AggregateEventRecord aggregate records} are never updated, and ID does not act
     * as a {@code PRIMARY KEY} in the table. That's why this method redirects to the
     * {@link #newInsertQuery(Object, AggregateEventRecord)} method.
     *
     * @return the result of
     * the {@linkplain #newInsertQuery(Object, AggregateEventRecord) newInsertQuery} method
     */
    @Override
    public WriteQuery newUpdateQuery(I id, AggregateEventRecord record) {
        logger.warn("UPDATE operation is not possible within the AggregateEventRecordTable. " +
                    "Performing an INSERT instead.");
        return newInsertQuery(id, record);
    }
}
