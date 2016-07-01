package org.spine3.server.storage.jdbc.entity.query;


import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EntityStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final String tableName;

    public EntityStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> entityClass) {
        this.idColumn = IdColumn.newInstance(entityClass);
        this.dataSource = dataSource;
        this.tableName = DbTableNameFactory.newTableName(entityClass);
    }

    public CreateTableQuery newCreateTableQuery() {
        return CreateTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setIdColumn(idColumn)
                .setTableName(tableName)
                .build();
    }

    public UpdateEntityQuery newUpdateEntityQuery(I id, EntityStorageRecord record) {
        return UpdateEntityQuery.<I>newBuilder(tableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public InsertEntityQuery newInsertEntityQuery(I id, EntityStorageRecord record) {
        return InsertEntityQuery.<I>newBuilder(tableName)
                .setId(id)
                .setIdColumn(idColumn)
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public SelectEntityByIdQuery <I> newSelectEntityByIdQuery(I id){
        return new SelectEntityByIdQuery<>(tableName, dataSource, idColumn, id);
    }

    public DeleteAllQuery newDeleteAllQuery(){
        return DeleteAllQuery.newBuilder(tableName)
                .setDataSource(dataSource)
                .build();
    }
}
