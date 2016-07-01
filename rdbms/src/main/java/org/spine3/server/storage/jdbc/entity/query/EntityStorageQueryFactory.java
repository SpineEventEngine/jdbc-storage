package org.spine3.server.storage.jdbc.entity.query;


import org.slf4j.Logger;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EntityStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final String tableName;
    private Logger logger;

    public EntityStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> entityClass) {
        this.idColumn = IdColumn.newInstance(entityClass);
        this.dataSource = dataSource;
        this.tableName = DbTableNameFactory.newTableName(entityClass);
    }

    public CreateTableQuery newCreateTableQuery() {
        final CreateTableQuery.Builder<I> builder = CreateTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setTableName(tableName);

        return builder.build();
    }

    public UpdateEntityQuery newUpdateEntityQuery(I id, EntityStorageRecord record) {
        final UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record);

        return builder.build();
    }

    public InsertEntityQuery newInsertEntityQuery(I id, EntityStorageRecord record) {
        final InsertEntityQuery.Builder<I> builder = InsertEntityQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setId(id)
                .setIdColumn(idColumn)
                .setRecord(record);

        return builder.build();
    }

    public SelectEntityByIdQuery <I> newSelectEntityByIdQuery(I id){
        return new SelectEntityByIdQuery<>(tableName, dataSource, idColumn, id);
    }

    public DeleteAllQuery newDeleteAllQuery(){
        final DeleteAllQuery.Builder builder = DeleteAllQuery.newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger);

        return builder.build();
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }
}
