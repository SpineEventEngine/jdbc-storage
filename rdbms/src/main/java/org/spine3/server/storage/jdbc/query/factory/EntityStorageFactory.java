package org.spine3.server.storage.jdbc.query.factory;


import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.tables.entity.*;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.query.tables.entity.SelectEntityByIdQuery;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EntityStorageFactory<I> {

    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final Class<? extends Entity<I, ?>> entityClass;
    private final String tableName;

    public EntityStorageFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> entityClass) {
        this.idColumn = IdColumn.newInstance(entityClass);
        this.dataSource = dataSource;
        this.entityClass = entityClass;
        this.tableName = DbTableNameFactory.newTableName(entityClass);
    }

    public CreateTableIfDoesNotExistQuery getCreateTableIfDoesNotExistQuery() {
        return CreateTableIfDoesNotExistQuery.<I>getBuilder()
                .setDataSource(dataSource)
                .setIdColumn(idColumn)
                .setTableName(tableName)
                .build();
    }

    public UpdateEntityQuery getUpdateEntityQuery(I id, EntityStorageRecord record) {
        return UpdateEntityQuery.<I>getBuilder(tableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public InsertEntityQuery getInsertEntityQuery(I id, EntityStorageRecord record) {
        return InsertEntityQuery.<I>getBuilder(tableName)
                .setId(id)
                .setIdColumn(idColumn)
                .setRecord(record)
                .setDataSource(dataSource)
                .build();
    }

    public SelectEntityByIdQuery <I> getSelectEntityByIdQuery(I id){
        return new SelectEntityByIdQuery<>(tableName, dataSource, idColumn, id);
    }

    public DeleteAllQuery getDeleteAllQuery(){
        return DeleteAllQuery.getBuilder(tableName)
                .setDataSource(dataSource)
                .build();
    }
}
