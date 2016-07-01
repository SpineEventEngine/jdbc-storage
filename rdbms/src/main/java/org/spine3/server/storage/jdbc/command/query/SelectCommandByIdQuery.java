/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.spine3.server.storage.jdbc.command.query;

import org.spine3.Internal;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.command.query.Constants.*;
import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;

@Internal
public class SelectCommandByIdQuery extends SelectByIdQuery<String, CommandStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_QUERY =
                    "SELECT * FROM " + TABLE_NAME +
                    " WHERE " + ID_COL + " = ?;";

    public SelectCommandByIdQuery(DataSourceWrapper dataSource, String id) {
        super(SELECT_QUERY, dataSource, new IdColumn.StringIdColumn(), id);
    }

    @Nullable
    @Override
    @SuppressWarnings("RefusedBequest")
    protected CommandStorageRecord readMessage(ResultSet resultSet) throws SQLException {
        final byte[] recordBytes = resultSet.getBytes(COMMAND_COL);
        if (recordBytes == null) {
            return null;
        }
        final CommandStorageRecord record = deserialize(recordBytes, COMMAND_RECORD_DESCRIPTOR);
        final CommandStorageRecord.Builder builder = record.toBuilder();
        final String status = resultSet.getString(COMMAND_STATUS_COL);
        if (status.equals(CommandStatus.forNumber(CommandStatus.OK_VALUE).name())) {
            return builder.setStatus(CommandStatus.OK).build();
        }
        final byte[] errorBytes = resultSet.getBytes(ERROR_COL);
        if (errorBytes != null) {
            final Error error = deserialize(errorBytes, ERROR_DESCRIPTOR);
            return builder.setError(error)
                    .setStatus(CommandStatus.ERROR)
                    .build();
        }
        final byte[] failureBytes = resultSet.getBytes(FAILURE_COL);
        if (failureBytes != null) {
            final Failure failure = deserialize(failureBytes, FAILURE_DESCRIPTOR);
            return builder.setFailure(failure)
                    .setStatus(CommandStatus.FAILURE)
                    .build();
        }
        return builder.build();
    }
}