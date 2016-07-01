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
    private Logger logger;

    public ProjectionStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> projectionClass) {
        this.idColumn = IdColumn.newInstance(projectionClass);
        this.tableName = newTableName(projectionClass) + LAST_EVENT_TIME_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public CreateTableQuery newCreateTableQuery() {
        final CreateTableQuery.Builder builder = CreateTableQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(tableName);

        return builder.build();
    }

    public InsertTimestampQuery newInsertTimestampQuery(Timestamp time) {
        final InsertTimestampQuery.Builder builder = InsertTimestampQuery.newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTimestamp(time);

        return builder.build();
    }

    public UpdateTimestampQuery newUpdateTimestampQuery(Timestamp time) {
        final UpdateTimestampQuery.Builder builder = UpdateTimestampQuery.newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTimestamp(time);

        return builder.build();
    }

    public SelectTimestampQuery newSelectTimestampQuery() {
        final SelectTimestampQuery.Builder builder = SelectTimestampQuery.newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger);

        return builder.build();
    }
}
