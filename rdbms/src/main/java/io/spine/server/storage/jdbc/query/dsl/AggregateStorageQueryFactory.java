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
import io.spine.server.storage.jdbc.DbIterator;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.StorageIndexQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import org.slf4j.Logger;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The query factory for interaction with
 * {@link io.spine.server.storage.jdbc.AggregateEventRecordTable AggregateEventRecordTable}.
 *
 * @param <I> the type of IDs used in the storage
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
@Internal
public class AggregateStorageQueryFactory<I> implements ReadQueryFactory<I, AggregateEventRecord>,
                                                        WriteQueryFactory<I, AggregateEventRecord> {

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final DataSourceWrapper dataSource;

    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     */
    public AggregateStorageQueryFactory(DataSourceWrapper dataSource,
                                        String tableName,
                                        IdColumn<I> idColumn) {
        super();
        this.idColumn = checkNotNull(idColumn);
        this.mainTableName = tableName;
        this.dataSource = dataSource;
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = checkNotNull(logger);
    }

    public Logger getLogger() {
        return logger;
    }

    /** Returns a query that selects aggregate records by ID sorted by time descending. */
    @SuppressWarnings("InstanceMethodNamingConvention")
    public SelectByIdQuery<I, DbIterator<AggregateEventRecord>> newSelectEventRecordsById(I id,
                                                                                          int fetchSize) {
        final SelectEventRecordsById.Builder<I> builder = SelectEventRecordsById.newBuilder();
        return builder.setTableName(mainTableName)
                      .setDataSource(dataSource)
                      .setIdColumn(idColumn)
                      .setId(id)
                      .setFetchSize(fetchSize)
                      .build();
    }

    /**
     * Thrown an {@link UnsupportedOperationException}.
     *
     * @deprecated multiple records correspond to a single ID in
     * {@link io.spine.server.storage.jdbc.AggregateEventRecordTable AggregateEventRecordTable};
     * please use {@link #newSelectEventRecordsById(Object, int)} to read the records.
     */
    @Deprecated
    @Override
    public SelectByIdQuery<I, AggregateEventRecord> newSelectByIdQuery(I id) {
        throw new UnsupportedOperationException("Use newSelectEventRecordsById instead.");
    }

    @Override
    public SelectQuery<Iterator<I>> newIndexQuery() {
        final StorageIndexQuery.Builder<I> builder = StorageIndexQuery.newBuilder();
        return builder.setDataSource(dataSource)
                      .setLogger(logger)
                      .setIdType(idColumn.getJavaType())
                      .setTableName(mainTableName)
                      .build();
    }

    @Override
    public WriteQuery newInsertQuery(I id, AggregateEventRecord record) {
        final InsertAggregateRecordQuery.Builder<I> builder = InsertAggregateRecordQuery.newBuilder();
        return builder.setTableName(mainTableName)
                      .setDataSource(dataSource)
                      .setIdColumn(idColumn)
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
