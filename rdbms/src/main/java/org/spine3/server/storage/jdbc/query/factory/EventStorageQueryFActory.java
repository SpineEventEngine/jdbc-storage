/*
package org.spine3.server.storage.jdbc.query.factory;


import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.query.tables.aggregate.CreateMainTableIfDoesNotExistQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EventStorageQueryFactory {

    private final IdColumn<String> idColumn = new IdColumn.StringIdColumn();
    private final String mainTableName;
    private final DataSourceWrapper dataSource;
    private final Class<? extends Entity<I, ?>> entityClass;

    public EntityStorageFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> entityClass) {
        super(dataSource);
        this.idColumn = IdColumn.newInstance(entityClass);
        this.mainTableName = DbTableNameFactory.newTableName(entityClass);
        this.dataSource = dataSource;
        this.entityClass = entityClass;
    }

    public CreateMainTableIfDoesNotExistQuery getCreateTableIfDoesNotExistQuery() {
        return CreateMainTableIfDoesNotExistQuery.getBuilder()
                .setTableName(mainTableName)
                .setIdType(idColumn.getColumnDataType())
                .setDataSource(dataSource)
                .build();
    }
}
*/
