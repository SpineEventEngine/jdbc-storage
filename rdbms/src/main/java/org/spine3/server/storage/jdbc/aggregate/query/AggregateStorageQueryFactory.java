package org.spine3.server.storage.jdbc.aggregate.query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class AggregateStorageQueryFactory<I>{

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final String eventCountTableName;
    private final DataSourceWrapper dataSource;

    public AggregateStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + AggregateTable.EVENT_COUNT_TABLE_NAME_SUFFIX;
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

    public CreateMainTableIfDoesNotExistQuery getCreateMainTableIfDoesNotExistQuery() {
        return CreateMainTableIfDoesNotExistQuery.<I>getBuilder()
                .setTableName(mainTableName)
                .setIdColumn(idColumn)
                .setDataSource(dataSource)
                .build();
    }

    public CreateEventCountTableIfDoesNotExistQuery getCreateEventCountTableIfDoesNotExistQuery() {
        return CreateEventCountTableIfDoesNotExistQuery.<I>getBuilder()
                .setTableName(eventCountTableName)
                .setIdColumn(idColumn)
                .setDataSource(dataSource)
                .build();
    }

    public InsertEventCountQuery getInsertEventCountQuery(I id, int count){
        return InsertEventCountQuery.<I>getBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count)
                .setDataSource(dataSource)
                .build();
    }

    public UpdateEventCountQuery getUpdateEventCountQuery(I id, int count){
        return UpdateEventCountQuery.<I>getBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setCount(count)
                .setDataSource(dataSource)
                .build();
    }

    public InsertRecordQuery getInsertRecordQuery(I id, AggregateStorageRecord record){
        return InsertRecordQuery.<I>getBuilder(mainTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public SelectEventCountByIdQuery getSelectEventCountByIdQuery(I id){
        return SelectEventCountByIdQuery.<I>getBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setDataSource(dataSource)
                .build();
    }

    public SelectByIdSortedByTimeDescQuery<I> getSelectByIdSortedByTimeDescQuery(I id){
        return SelectByIdSortedByTimeDescQuery.<I>getBuilder(mainTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setDataSource(dataSource)
                .build();
    }

}
