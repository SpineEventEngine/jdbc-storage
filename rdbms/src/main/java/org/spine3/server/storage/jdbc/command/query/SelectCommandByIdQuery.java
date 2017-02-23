/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.command.CommandRecord;
import org.spine3.server.command.ProcessingStatus;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.ALL_ATTRIBUTES;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.COMMAND_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.COMMAND_STATUS_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.ERROR_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.FAILURE_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.ID_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.TABLE_NAME;
import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;

/**
 * Query that selects {@link CommandRecord} by ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectCommandByIdQuery extends SelectByIdQuery<String, CommandRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            SELECT.toString() + ALL_ATTRIBUTES + FROM + TABLE_NAME +
            WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    public SelectCommandByIdQuery(Builder builder) {
        super(builder);
    }

    @Nullable
    @Override
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    // We store the Commands not the same way as other records
    protected CommandRecord readMessage(ResultSet resultSet) throws SQLException {
        final byte[] recordBytes = resultSet.getBytes(COMMAND_COL);
        if (recordBytes == null) {
            return null;
        }
        final CommandRecord record = deserialize(recordBytes,
                                                        CommandRecord.getDescriptor());
        final CommandRecord.Builder builder = record.toBuilder();
        final String status = resultSet.getString(COMMAND_STATUS_COL);
        if (status.equals(CommandStatus.forNumber(CommandStatus.OK_VALUE)
                                       .name())) {
            final ProcessingStatus statusOk = ProcessingStatus.newBuilder().setCode(CommandStatus.OK).build();
            return builder.setStatus(statusOk)
                          .build();
        }
        final byte[] errorBytes = resultSet.getBytes(ERROR_COL);
        if (errorBytes != null) {
            final Error error = deserialize(errorBytes, Error.getDescriptor());
            final ProcessingStatus statusError = ProcessingStatus.newBuilder()
                                                              .setCode(CommandStatus.ERROR)
                                                              .setError(error)
                                                              .build();
            return builder.setStatus(statusError)
                          .build();
        }
        final byte[] failureBytes = resultSet.getBytes(FAILURE_COL);
        if (failureBytes != null) {
            final Failure failure = deserialize(failureBytes, Failure.getDescriptor());
            final ProcessingStatus statusFailure = ProcessingStatus.newBuilder()
                                                              .setCode(CommandStatus.FAILURE)
                                                              .setFailure(failure)
                                                              .build();
            return builder.setStatus(statusFailure)
                          .build();
        }
        return builder.build();
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setIdIndexInQuery(1)
               .setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends SelectByIdQuery.Builder<Builder, SelectCommandByIdQuery, String, CommandRecord> {

        @Override
        public SelectCommandByIdQuery build() {
            return new SelectCommandByIdQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
