package org.spine3.server.storage.jdbc.projection.query;


import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;
import static org.spine3.server.storage.jdbc.projection.query.Constants.*;

import static org.spine3.server.storage.jdbc.util.DbTableNameFactory.newTableName;

public class ProjectionStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final String tableName;
    private final DataSourceWrapper dataSource;

    public ProjectionStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> projectionClass) {
        this.idColumn = IdColumn.newInstance(projectionClass);
        this.tableName = newTableName(projectionClass) + LAST_EVENT_TIME_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(ProjectionStorageQueryFactory.class);
    }

    public CreateTableQuery newCreateTableQuery() {
        return CreateTableQuery.newBuilder()
                .setTableName(tableName)
                .setDataSource(dataSource)
                .build();
    }

    public InsertTimestampQuery newInsertTimestampQuery(Timestamp time) {
        return InsertTimestampQuery.newBuilder(tableName)
                .setTimestamp(time)
                .setDataSource(dataSource)
                .build();
    }

    public UpdateTimestampQuery newUpdateTimestampQuery(Timestamp time) {
        return UpdateTimestampQuery.newBuilder(tableName)
                .setTimestamp(time)
                .setDataSource(dataSource)
                .build();
    }

    public SelectTimestampQuery newSelectTimestampQuery() {
        return SelectTimestampQuery.newBuilder(tableName)
                .setDataSource(dataSource)
                .build();
    }
}
