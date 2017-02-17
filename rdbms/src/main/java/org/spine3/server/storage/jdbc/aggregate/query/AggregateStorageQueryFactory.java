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
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.aggregate.storage.AggregateStorageRecord;
import org.spine3.server.entity.status.EntityStatus;
import org.spine3.server.storage.jdbc.entity.status.EntityStatusHandlingStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.status.query.CreateEntityStatusTableQuery;
import org.spine3.server.storage.jdbc.entity.status.query.InsertEntityStatusQuery;
import org.spine3.server.storage.jdbc.entity.status.query.SelectEntityStatusQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.aggregate.query.Table.EventCount.EVENT_COUNT_TABLE_NAME_SUFFIX;

/**
 * This class creates queries for interaction with {@link Table.AggregateRecord} and {@link Table.EventCount}.
 *
 * @param <I> the type of IDs used in the storage
 * @author Andrey Lavrov
 */
public class AggregateStorageQueryFactory<I> implements EntityStatusHandlingStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final String eventCountTableName;
    private final DataSourceWrapper dataSource;
    private final EntityStatusHandlingStorageQueryFactory<I> statusTableQueryFactory;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource     instance of {@link DataSourceWrapper}
     * @param aggregateClass aggregate class of corresponding {@link AggregateStorage} instance
     */
    public AggregateStorageQueryFactory(DataSourceWrapper dataSource,
                                        Class<? extends Aggregate<I, ?, ?>> aggregateClass,
                                        EntityStatusHandlingStorageQueryFactory<I> statusTableQueryFactory) {
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
        this.statusTableQueryFactory = statusTableQueryFactory;
    }

    @Override
    public CreateEntityStatusTableQuery newCreateEntityStatusTableQuery() {
        return statusTableQueryFactory.newCreateEntityStatusTableQuery();
    }

    @Override
    public InsertEntityStatusQuery newInsertEntityStatusQuery(I id, EntityStatus entityStatus) {
        return statusTableQueryFactory.newInsertEntityStatusQuery(id, entityStatus);
    }

    @Override
    public SelectEntityStatusQuery newSelectEntityStatusQuery(I id) {
        return statusTableQueryFactory.newSelectEntityStatusQuery(id);
    }

    /** Sets the logger for logging exceptions during queries execution. */
    @Override
    public void setLogger(Logger logger) {
        this.logger = logger;
        statusTableQueryFactory.setLogger(logger);
    }

    /** Returns a query that creates a new {@link Table.AggregateRecord} if it does not exist. */
    public CreateMainTableQuery newCreateMainTableQuery() {
        final CreateMainTableQuery.Builder<I> builder = CreateMainTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(mainTableName)
                .setIdColumn(idColumn);
        return builder.build();
    }

    /** Returns a query that creates a new {@link Table.EventCount} if it does not exist. */
    public CreateEventCountTableQuery newCreateEventCountTableQuery() {
        final CreateEventCountTableQuery.Builder<I> builder = CreateEventCountTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(eventCountTableName)
                .setIdColumn(idColumn);
        return builder.build();
    }

    /**
     * Returns a  query that inserts a new aggregate event count after the last snapshot to the {@link Table.EventCount}.
     *
     * @param id    corresponding aggregate id
     * @param count event count
     */
    public InsertEventCountQuery newInsertEventCountQuery(I id, int count) {
        final InsertEventCountQuery.Builder<I> builder = InsertEventCountQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count);
        return builder.build();
    }

    /**
     * Returns a query that updates aggregate event count in the {@link Table.EventCount}.
     *
     * @param id    corresponding aggregate id
     * @param count new event count
     */
    public UpdateEventCountQuery newUpdateEventCountQuery(I id, int count) {
        final UpdateEventCountQuery.Builder<I> builder = UpdateEventCountQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count);
        return builder.build();
    }

    /**
     * Returns a query that inserts a new {@link AggregateStorageRecord} to the {@link Table.AggregateRecord}.
     *
     * @param id     aggregate id
     * @param record new aggregate record
     */
    public InsertAggregateRecordQuery newInsertRecordQuery(I id, AggregateStorageRecord record) {
        final InsertAggregateRecordQuery.Builder<I> builder = InsertAggregateRecordQuery.<I>newBuilder(mainTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record);
        return builder.build();
    }

    /** Returns a query that selects event count by corresponding aggregate ID. */
    public SelectEventCountByIdQuery<I> newSelectEventCountByIdQuery(I id) {
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);
        return builder.build();
    }

    /** Returns a query that selects aggregate records by ID sorted by time descending. */
    @SuppressWarnings("InstanceMethodNamingConvention")
    public SelectByIdSortedByTimeDescQuery<I> newSelectByIdSortedByTimeDescQuery(I id) {
        final SelectByIdSortedByTimeDescQuery.Builder<I> builder = SelectByIdSortedByTimeDescQuery.<I>newBuilder(mainTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);
        return builder.build();
    }
}
