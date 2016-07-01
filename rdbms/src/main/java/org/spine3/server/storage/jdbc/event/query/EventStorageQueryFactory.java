package org.spine3.server.storage.jdbc.event.query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EventStorageQueryFactory{

    private final IdColumn idColumn;
    private final DataSourceWrapper dataSource;

    public EventStorageQueryFactory(DataSourceWrapper dataSource) {
        this.idColumn = new IdColumn.StringIdColumn();
        this.dataSource = dataSource;
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(EventStorageQueryFactory.class);
    }

    public CreateTableIfDoesNotExistQuery getCreateTableIfDoesNotExistQuery(){
        return CreateTableIfDoesNotExistQuery.newBuilder()
                .setDataSource(dataSource)
                .build();
    }

    public InsertEventQuery getInsertEventQuery(EventStorageRecord record){
        return InsertEventQuery.newBuilder()
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public UpdateEventQuery getUpdateEventQuery(EventStorageRecord record){
        return UpdateEventQuery.newBuilder()
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public SelectEventByIdQuery getSelectEventByIdQuery(String id){
        return new SelectEventByIdQuery(dataSource, id);
    }

    public FilterAndSortQuery getFilterAndSortQuery(EventStreamQuery streamQuery){
        return FilterAndSortQuery.newBuilder()
                .setStreamQuery(streamQuery)
                .setDataSource(dataSource)
                .build();
    }
}
