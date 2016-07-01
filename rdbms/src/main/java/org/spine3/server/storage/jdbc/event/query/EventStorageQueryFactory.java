package org.spine3.server.storage.jdbc.event.query;


import org.slf4j.Logger;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EventStorageQueryFactory{

    private final DataSourceWrapper dataSource;
    private final IdColumn<String> idColumn;
    private Logger logger;

    public EventStorageQueryFactory(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
        this.idColumn = new IdColumn.StringIdColumn();
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public CreateTableQuery newCreateTableQuery(){
        final CreateTableQuery.Builder builder = CreateTableQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger);

        return builder.build();
    }

    public InsertEventQuery newInsertEventQuery(EventStorageRecord record){
        final InsertEventQuery.Builder builder = InsertEventQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setRecord(record);

        return builder.build();
    }

    public UpdateEventQuery newUpdateEventQuery(EventStorageRecord record){
        final UpdateEventQuery.Builder builder = UpdateEventQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setRecord(record);

        return builder.build();
    }

    public SelectEventByIdQuery newSelectEventByIdQuery(String id){
        final SelectEventByIdQuery.Builder builder = SelectEventByIdQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);

        return builder.build();
    }

    public FilterAndSortQuery newFilterAndSortQuery(EventStreamQuery streamQuery){
        final FilterAndSortQuery.Builder builder = FilterAndSortQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setStreamQuery(streamQuery);

        return builder.build();
    }
}
