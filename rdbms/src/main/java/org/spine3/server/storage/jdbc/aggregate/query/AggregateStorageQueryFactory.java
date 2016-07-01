package org.spine3.server.storage.jdbc.aggregate.query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public AggregateStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(AggregateStorageQueryFactory.class);
    }

    public CreateMainTableQuery newCreateMainTableQuery() {
        final CreateMainTableQuery.Builder<I> builder = CreateMainTableQuery.<I>newBuilder()
                .setTableName(mainTableName)
                .setIdColumn(idColumn)
                .setDataSource(dataSource);

        return builder.build();
    }

    public CreateEventCountQuery newCreateEventCountTableQuery() {
        final CreateEventCountQuery.Builder<I> builder = CreateEventCountQuery.<I>newBuilder()
                .setTableName(eventCountTableName)
                .setIdColumn(idColumn)
                .setDataSource(dataSource);

        return builder.build();
    }

    public InsertEventCountQuery newInsertEventCountQuery(I id, int count){
        final InsertEventCountQuery.Builder<I> builder = InsertEventCountQuery.<I>newBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count)
                .setDataSource(dataSource);

        return builder.build();
    }

    public UpdateEventCountQuery newUpdateEventCountQuery(I id, int count){
        final UpdateEventCountQuery.Builder<I> builder = UpdateEventCountQuery.<I>newBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count)
                .setDataSource(dataSource);

        return builder.build();
    }

    public InsertRecordQuery newInsertRecordQuery(I id, AggregateStorageRecord record){
        final InsertRecordQuery.Builder<I> builder = InsertRecordQuery.<I>newBuilder(mainTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record)
                .setDataSource(dataSource);

        return builder.build();
    }

    public SelectEventCountByIdQuery newSelectEventCountByIdQuery(I id){
        final SelectEventCountByIdQuery.Builder<I> builder = SelectEventCountByIdQuery.<I>newBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setDataSource(dataSource);

        return builder.build();
    }

    @SuppressWarnings("InstanceMethodNamingConvention")
    public SelectByIdSortedByTimeDescQuery<I> newSelectByIdSortedByTimeDescQuery(I id){
        final SelectByIdSortedByTimeDescQuery.Builder<I> builder = SelectByIdSortedByTimeDescQuery.<I>newBuilder(mainTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setDataSource(dataSource);

        return builder.build();
    }

}
