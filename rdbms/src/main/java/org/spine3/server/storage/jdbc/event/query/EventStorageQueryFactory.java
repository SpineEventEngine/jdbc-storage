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

    public CreateTableQuery newCreateTableQuery(){
        return CreateTableQuery.newBuilder()
                .setDataSource(dataSource)
                .build();
    }

    public InsertEventQuery newInsertEventQuery(EventStorageRecord record){
        return InsertEventQuery.newBuilder()
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public UpdateEventQuery newUpdateEventQuery(EventStorageRecord record){
        return UpdateEventQuery.newBuilder()
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public SelectEventByIdQuery newSelectEventByIdQuery(String id){
        return new SelectEventByIdQuery(dataSource, id);
    }

    public FilterAndSortQuery newFilterAndSortQuery(EventStreamQuery streamQuery){
        return FilterAndSortQuery.newBuilder()
                .setStreamQuery(streamQuery)
                .setDataSource(dataSource)
                .build();
    }
}
