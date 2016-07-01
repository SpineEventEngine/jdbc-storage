package org.spine3.server.storage.jdbc.aggregate.query;


import org.slf4j.Logger;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.aggregate.query.Constants.EVENT_COUNT_TABLE_NAME_SUFFIX;

public class AggregateStorageQueryFactory<I>{

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final String eventCountTableName;
    private final DataSourceWrapper dataSource;
    private Logger logger;

    public AggregateStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public CreateMainTableQuery newCreateMainTableQuery() {
        final CreateMainTableQuery.Builder<I> builder = CreateMainTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(mainTableName)
                .setIdColumn(idColumn);

        return builder.build();
    }

    public CreateEventCountQuery newCreateEventCountTableQuery() {
        final CreateEventCountQuery.Builder<I> builder = CreateEventCountQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(eventCountTableName)
                .setIdColumn(idColumn);

        return builder.build();
    }

    public InsertEventCountQuery newInsertEventCountQuery(I id, int count){
        final InsertEventCountQuery.Builder<I> builder = InsertEventCountQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count);

        return builder.build();
    }

    public UpdateEventCountQuery newUpdateEventCountQuery(I id, int count){
        final UpdateEventCountQuery.Builder<I> builder = UpdateEventCountQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count);

        return builder.build();
    }

    public InsertRecordQuery newInsertRecordQuery(I id, AggregateStorageRecord record){
        final InsertRecordQuery.Builder<I> builder = InsertRecordQuery.<I>newBuilder(mainTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record);

        return builder.build();
    }

    public SelectEventCountByIdQuery newSelectEventCountByIdQuery(I id){
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.<I>newBuilder(eventCountTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);

        return builder.build();
    }

    @SuppressWarnings("InstanceMethodNamingConvention")
    public SelectByIdSortedByTimeDescQuery<I> newSelectByIdSortedByTimeDescQuery(I id){
        final SelectByIdSortedByTimeDescQuery.Builder<I> builder = SelectByIdSortedByTimeDescQuery.<I>newBuilder(mainTableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);

        return builder.build();
    }
}
