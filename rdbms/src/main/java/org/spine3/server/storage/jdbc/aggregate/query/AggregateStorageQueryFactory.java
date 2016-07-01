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
 * This class provides public methods for creating the most commonly used queries.
 * To use you custom queries extend this class and overwrite necessary methods.
 * See {@link JdbcStorageFactory} to see how to pass your custom factory to the storage
 *
 * @param <I> ID type of the Aggregate {@link AggregateStorage} for which this query factory is created.
 */
public class AggregateStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final String eventCountTableName;
    private final DataSourceWrapper dataSource;
    private Logger logger;

    /**
     *
     * @param dataSource the dataSource wrapper
     * @param aggregateClass class of aggregate of the owning {@link AggregateStorage}
     */
    public AggregateStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
    }

    /**
     * Use this method to path your custom loggers if you decide to overwrite {@link AggregateStorage}
     *
     * @param logger logger which will be used to log exceptions during queries execution
     */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Returns a query which creates the new query witch creates new aggregate Main table if it does not exist
     *
     * If you overwrite this method with your custom query keep in mind that default {@link AggregateStorage}
     * realisation supposes that it creates a new table only if it does not exist
     */
    public CreateMainTableQuery newCreateMainTableQuery() {
        final CreateMainTableQuery.Builder<I> builder = CreateMainTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(mainTableName)
                .setIdColumn(idColumn);

        return builder.build();
    }

    /**
     * Returns a query which creates the new query witch creates new aggregate EventCount table if it does not exist
     *
     * If you overwrite this method with your custom query keep in mind that default {@link AggregateStorage}
     * realisation supposes that it creates a new table only if it does not exist
     */
    public CreateEventCountQuery newCreateEventCountTableQuery() {
        final CreateEventCountQuery.Builder<I> builder = CreateEventCountQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(eventCountTableName)
                .setIdColumn(idColumn);

        return builder.build();
    }

    public InsertEventCountQuery newInsertEventCountQuery(I id, int count) {
        final InsertEventCountQuery.Builder<I> builder = InsertEventCountQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count);

        return builder.build();
    }

    public UpdateEventCountQuery newUpdateEventCountQuery(I id, int count) {
        final UpdateEventCountQuery.Builder<I> builder = UpdateEventCountQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count);

        return builder.build();
    }

    public InsertRecordQuery newInsertRecordQuery(I id, AggregateStorageRecord record) {
        final InsertRecordQuery.Builder<I> builder = InsertRecordQuery.<I>newBuilder(mainTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record);

        return builder.build();
    }

    public SelectEventCountByIdQuery newSelectEventCountByIdQuery(I id) {
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);

        return builder.build();
    }

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
