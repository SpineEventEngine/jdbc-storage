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
import org.spine3.server.storage.jdbc.table.CommandTable;

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
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.command;
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.command_status;
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.error;
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.failure;
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.id;
import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;

/**
 * Query that selects {@link CommandRecord} by ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectCommandByIdQuery extends SelectByIdQuery<String, CommandRecord> {

    private static final String QUERY_TEMPLATE =
            SELECT.toString() + ALL_ATTRIBUTES + FROM + CommandTable.TABLE_NAME +
            WHERE + id + EQUAL + PLACEHOLDER + SEMICOLON;

    public SelectCommandByIdQuery(Builder builder) {
        super(builder);
    }

    @Nullable
    @Override
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    // We store the Commands not the same way as other records
    protected CommandRecord readMessage(ResultSet resultSet) throws SQLException {
        final byte[] recordBytes = resultSet.getBytes(command.name());
        if (recordBytes == null) {
            return null;
        }
        final CommandRecord record = deserialize(recordBytes,
                                                 CommandRecord.getDescriptor());
        final CommandRecord.Builder builder = record.toBuilder();
        final String status = resultSet.getString(command_status.name());
        if (status.equals(CommandStatus.OK.name())) {
            final ProcessingStatus statusOk = ProcessingStatus.newBuilder()
                                                              .setCode(CommandStatus.OK)
                                                              .build();
            final CommandRecord result = builder.setStatus(statusOk)
                                                .build();
            return result;
        }
        final byte[] errorBytes = resultSet.getBytes(error.name());
        if (errorBytes != null) {
            final Error error = deserialize(errorBytes, Error.getDescriptor());
            final ProcessingStatus statusError = ProcessingStatus.newBuilder()
                                                                 .setCode(CommandStatus.ERROR)
                                                                 .setError(error)
                                                                 .build();
            final CommandRecord result = builder.setStatus(statusError)
                                                .build();
            return result;
        }
        final byte[] failureBytes = resultSet.getBytes(failure.name());
        if (failureBytes != null) {
            final Failure failure = deserialize(failureBytes, Failure.getDescriptor());
            final ProcessingStatus statusFailure = ProcessingStatus.newBuilder()
                                                                   .setCode(CommandStatus.FAILURE)
                                                                   .setFailure(failure)
                                                                   .build();
            final CommandRecord result = builder.setStatus(statusFailure)
                                                .build();
            return result;
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
    public static class Builder extends SelectByIdQuery.Builder<Builder,
                                                                SelectCommandByIdQuery,
                                                                String,
                                                                CommandRecord> {

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
