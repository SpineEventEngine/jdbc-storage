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

@SuppressWarnings("TypeMayBeWeakened")
public class CommandStorageQueryFactory {

    private final IdColumn <String> idColumn;
    private final DataSourceWrapper dataSource;
    private Logger logger;

    public CommandStorageQueryFactory(DataSourceWrapper dataSource) {
        this.idColumn = new IdColumn.StringIdColumn();
        this.dataSource = dataSource;
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

    public InsertCommandQuery newInsertCommandQuery(CommandId id, CommandStorageRecord record){
        final InsertCommandQuery.Builder builder = InsertCommandQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(record)
                .setStatus(CommandStatus.forNumber(record.getStatusValue()));

        return builder.build();
    }

    public UpdateCommandQuery newUpdateCommandQuery(CommandId id, CommandStorageRecord record){
        final UpdateCommandQuery.Builder builder = UpdateCommandQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(record)
                .setStatus(CommandStatus.forNumber(record.getStatusValue()));

        return builder.build();
    }

    public SetErrorQuery newSetErrorQuery(CommandId id, Error error){
        final SetErrorQuery.Builder builder = SetErrorQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(error);

        return builder.build();
    }

    public SetFailureQuery newSetFailureQuery(CommandId id, Failure failure){
        final SetFailureQuery.Builder builder = SetFailureQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(failure);

        return builder.build();
    }

    public SetOkStatusQuery newSetOkStatusQuery(CommandId id){
        final SetOkStatusQuery.Builder builder = SetOkStatusQuery.newBuilder()
                .setDataSource(this.dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid());

        return builder.build();
    }

    public SelectCommandByIdQuery newSelectCommandByIdQuery(CommandId id){
        return new SelectCommandByIdQuery(dataSource, id.getUuid());
    }

    public SelectByStatusQuery newSelectByStatusQuery(CommandStatus status){
        final SelectByStatusQuery.Builder builder = SelectByStatusQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setStatus(status);

        return builder.build();
    }
}
