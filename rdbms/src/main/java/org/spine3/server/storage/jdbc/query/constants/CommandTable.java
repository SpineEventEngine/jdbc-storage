package org.spine3.server.storage.jdbc.query.constants;


import com.google.protobuf.Descriptors;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class CommandTable {
    public static final String TABLE_NAME = "commands";
    public static final String ID_COL = "id";
    public static final String COMMAND_COL = "command";
    public static final Descriptors.Descriptor COMMAND_RECORD_DESCRIPTOR = CommandStorageRecord.getDescriptor();
    public static final String IS_STATUS_OK_COL = "status_ok";
    public static final String COMMAND_STATUS_COL = "command_status";
    public static final String ERROR_COL = "error";
    public static final Descriptors.Descriptor ERROR_DESCRIPTOR = Error.getDescriptor();
    public static final String FAILURE_COL = "failure";
    public static final Descriptors.Descriptor FAILURE_DESCRIPTOR = Failure.getDescriptor();
    public static final IdColumn.StringIdColumn STRING_ID_COLUMN = new IdColumn.StringIdColumn();

    private CommandTable(){}
}
