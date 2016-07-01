package org.spine3.server.storage.jdbc.event.query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EventStorageQueryFactory{

    private final DataSourceWrapper dataSource;

    public EventStorageQueryFactory(DataSourceWrapper dataSource) {
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
        final CreateTableQuery.Builder builder = CreateTableQuery.newBuilder()
                .setDataSource(dataSource);

        return builder.build();
    }

    public InsertEventQuery newInsertEventQuery(EventStorageRecord record){
        final InsertEventQuery.Builder builder = InsertEventQuery.newBuilder()
                .setRecord(record)
                .setDataSource(dataSource);

        return builder.build();
    }

    public UpdateEventQuery newUpdateEventQuery(EventStorageRecord record){
        final UpdateEventQuery.Builder builder = UpdateEventQuery.newBuilder()
                .setRecord(record)
                .setDataSource(dataSource);

        return builder.build();
    }

    public SelectEventByIdQuery newSelectEventByIdQuery(String id){
        return new SelectEventByIdQuery(dataSource, id);
    }

    public FilterAndSortQuery newFilterAndSortQuery(EventStreamQuery streamQuery){
        final FilterAndSortQuery.Builder builder = FilterAndSortQuery.newBuilder()
                .setStreamQuery(streamQuery)
                .setDataSource(dataSource);

        return builder.build();
    }
}
