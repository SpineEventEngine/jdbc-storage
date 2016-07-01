package org.spine3.server.storage.jdbc.command.query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class CommandStorageQueryFactory {

    private final IdColumn <String> idColumn;
    private final DataSourceWrapper dataSource;

    public CommandStorageQueryFactory(DataSourceWrapper dataSource) {
        this.idColumn = new IdColumn.StringIdColumn();
        this.dataSource = dataSource;
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(CommandStorageQueryFactory.class);
    }

    public CreateTableQuery newCreateTableQuery(){
        return CreateTableQuery.newBuilder()
                .setDataSource(dataSource)
                .build();
    }

    public InsertCommandQuery newInsertCommandQuery(CommandId id, CommandStorageRecord record){
        return InsertCommandQuery.newBuilder()
                .setDataSource(dataSource)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(record)
                .setStatus(CommandStatus.forNumber(record.getStatusValue()))
                .build();
    }

    @SuppressWarnings("")
    public UpdateCommandQuery newUpdateCommandQuery(CommandId id, CommandStorageRecord record){
        return UpdateCommandQuery.newBuilder()
                .setDataSource(dataSource)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(record)
                .setStatus(CommandStatus.forNumber(record.getStatusValue()))
                .build();
    }

    public SetErrorQuery newSetErrorQuery(CommandId id, Error error){
        return SetErrorQuery.newBuilder()
                .setDataSource(dataSource)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(error)
                .build();
    }

    public SetFailureQuery newSetFailureQuery(CommandId id, Failure failure){
        return SetFailureQuery.newBuilder()
                .setDataSource(dataSource)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(failure)
                .build();
    }

    public SetOkStatusQuery newSetOkStatusQuery(CommandId id){
        return SetOkStatusQuery.newBuilder()
                .setDataSource(this.dataSource)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .build();
    }

    public SelectCommandByIdQuery newSelectCommandByIdQuery(CommandId id){
        return new SelectCommandByIdQuery(dataSource, id.getUuid());
    }

    public SelectByStatusQuery newSelectByStatusQuery(CommandStatus status){
        return SelectByStatusQuery.newBuilder()
                .setDataSource(dataSource)
                .setStatus(status)
                .build();
    }
}
