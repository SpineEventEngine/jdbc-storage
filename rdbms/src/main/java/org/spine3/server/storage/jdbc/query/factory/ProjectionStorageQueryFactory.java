package org.spine3.server.storage.jdbc.query.factory;


import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.query.constants.ProjectionTable;
import org.spine3.server.storage.jdbc.query.tables.projection.CreateTableIfDoesNotExistQuery;
import org.spine3.server.storage.jdbc.query.tables.projection.InsertTimestampQuery;
import org.spine3.server.storage.jdbc.query.tables.projection.SelectTimestampQuery;
import org.spine3.server.storage.jdbc.query.tables.projection.UpdateTimestampQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.util.DbTableNameFactory.newTableName;

public class ProjectionStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final String tableName;
    private final DataSourceWrapper dataSource;

    public ProjectionStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> projectionClass) {
        this.idColumn = IdColumn.newInstance(projectionClass);
        this.tableName = newTableName(projectionClass) + ProjectionTable.LAST_EVENT_TIME_TABLE_NAME_SUFFIX;
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

    public CreateTableIfDoesNotExistQuery getCreateTableIfDoesNotExistQuery() {
        return CreateTableIfDoesNotExistQuery.getBuilder()
                .setTableName(tableName)
                .setDataSource(dataSource)
                .build();
    }

    public InsertTimestampQuery getInsertTimestampQuery(Timestamp time) {
        return InsertTimestampQuery.getBuilder(tableName)
                .setTimestamp(time)
                .setDataSource(dataSource)
                .build();
    }

    public UpdateTimestampQuery getUpdateTimestampQuery(Timestamp time) {
        return UpdateTimestampQuery.getBuilder(tableName)
                .setTimestamp(time)
                .setDataSource(dataSource)
                .build();
    }

    public SelectTimestampQuery getSelectTimestampQuery() {
        return SelectTimestampQuery.getBuilder(tableName)
                .setDataSource(dataSource)
                .build();
    }
}
