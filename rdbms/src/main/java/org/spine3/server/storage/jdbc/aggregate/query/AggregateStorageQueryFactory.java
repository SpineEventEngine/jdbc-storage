/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.aggregate.query.Constants.EVENT_COUNT_TABLE_NAME_SUFFIX;

/**
 * This class creates the most commonly used queries.
 *
 * @param <I> Aggregate ID type
 * @author Andrey Lavrov
 */
public class AggregateStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final String eventCountTableName;
    private final DataSourceWrapper dataSource;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource the dataSource wrapper
     * @param aggregateClass class aggregate of the {@link AggregateStorage}
     */
    public AggregateStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
    }

    /**
     * Sets custom loggers.
     *
     * @param logger to log exceptions during queries execution
     */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /** Returns a query that creates a new aggregate Main table if it does not exist. */
    public CreateMainTableQuery newCreateMainTableQuery() {
        final CreateMainTableQuery.Builder<I> builder = CreateMainTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(mainTableName)
                .setIdColumn(idColumn);
        return builder.build();
    }

    /** Returns a query that creates a new aggregate EventCount table if it does not exist */
    public CreateEventCountTableQuery newCreateEventCountTableQuery() {
        final CreateEventCountTableQuery.Builder<I> builder = CreateEventCountTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(eventCountTableName)
                .setIdColumn(idColumn);
        return builder.build();
    }

    /**
     * Returns a query that inserts a new event count to EventCountTable
     *
     * @param id    corresponding aggregates id
     * @param count event count
     * @return      query that inserts new event count
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
     * Returns a query that updates event count in EventCountTable
     *
     * @param id    corresponding aggregates id
     * @param count event count
     * @return      query that updates new event count
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
     * Returns a query that inserts a new aggregate record to MainTable
     *
     * @param id     aggregates id
     * @param record new aggregate record
     * @return       query that inserts new aggregate record
     */
    public InsertRecordQuery newInsertRecordQuery(I id, AggregateStorageRecord record) {
        final InsertRecordQuery.Builder<I> builder = InsertRecordQuery.<I>newBuilder(mainTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record);
        return builder.build();
    }

    /**
     * Returns a query that selects event count by it`s ID.
     *
     * @param id corresponding aggregate id
     * @return   query that selects event count by id of the corresponding aggregate
     */
    public SelectEventCountByIdQuery newSelectEventCountByIdQuery(I id) {
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);
        return builder.build();
    }

    /**
     * Returns a query that selects aggregate records by ID sorted by time descending.
     *
     * @param id aggregate id
     * @return   query that selects aggregate records by ID sorted by time descending
     */
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
