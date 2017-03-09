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

import org.slf4j.Logger;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateEventRecord;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class creates queries for interaction with {@link Table.AggregateRecord} and {@link Table.EventCount}.
 *
 * @param <I> the type of IDs used in the storage
 * @author Andrey Lavrov
 */
public class AggregateStorageQueryFactory<I> implements QueryFactory<I, AggregateEventRecord> {

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final DataSourceWrapper dataSource;

    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource     instance of {@link DataSourceWrapper}
     * @param aggregateClass aggregate class of corresponding {@link AggregateStorage} instance
     */
    // The aux visibility handling query factory has a hardcoded type param
    public AggregateStorageQueryFactory(DataSourceWrapper dataSource,
                                        Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        super();
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
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
    public SelectByIdSortedByTimeDescQuery<I> newSelectByIdSortedByTimeDescQuery(I id) {
        final SelectByIdSortedByTimeDescQuery.Builder<I> builder =
                SelectByIdSortedByTimeDescQuery.<I>newBuilder(mainTableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id);
        return builder.build();
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated multiple records correspond to a single ID in
     * {@link org.spine3.server.storage.jdbc.table.entity.aggregate.AggregateEventRecordTable};
     * please use {@link #newSelectByIdSortedByTimeDescQuery(Object)} to read the records.
     */
    @Deprecated
    @Override
    public SelectByIdQuery<I, AggregateEventRecord> newSelectByIdQuery(I id) {
        throw new UnsupportedOperationException("Use newSelectByIdSortedByTimeDescQuery instead.");
    }

    @Override
    public WriteQuery newInsertQuery(I id, AggregateEventRecord record) {
        final InsertAggregateRecordQuery.Builder<I> builder =
                InsertAggregateRecordQuery.<I>newBuilder(mainTableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id)
                        .setRecord(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(I id, AggregateEventRecord record) {
        logger.warn("UPDATE operation is not possible within the AggregateEventRecordTable. Performing an INSERT instead.");
        return newInsertQuery(id, record);
    }
}
