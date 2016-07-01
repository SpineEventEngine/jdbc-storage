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
import org.spine3.base.Command;
import org.spine3.base.CommandStatus;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.Query;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static org.spine3.server.storage.jdbc.command.query.Constants.*;


/**
 * A query which obtains a {@link Command} by an command status.
 *
 * @author Andrey Lavrov
 */

@Internal
public class SelectByStatusQuery extends Query {

    private final CommandStatus status;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_BY_STATUS_QUERY =
            "SELECT " + COMMAND_COL + " FROM " + TABLE_NAME +
            " WHERE " + COMMAND_STATUS_COL + " = ?;";

    /**
     * Creates a new query instance.
     */
    protected SelectByStatusQuery(Builder builder) {
        super(builder);
        this.status = builder.status;
    }

    /**
     *
     */
    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(SELECT_BY_STATUS_QUERY);
        return builder;
    }

    /**
     * Prepares SQL statement using the connection.
     */
    @Override
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        try {
            statement.setString(1, status.toString());
        } catch (SQLException e) {
            this.getLogger().error("Failed to prepare statement ", e);
            throw new DatabaseException(e);
        }
        return statement;
    }

    public Iterator<CommandStorageRecord> execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.getConnection(true)) {
            final PreparedStatement statement = this.prepareStatement(connection);
            return new DbIterator<>(statement, COMMAND_COL, CommandStorageRecord.getDescriptor());
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends Query.Builder<Builder, SelectByStatusQuery> {

        private CommandStatus status;

        @Override
        public SelectByStatusQuery build() {
            return new SelectByStatusQuery(this);
        }

        public Builder setStatus(CommandStatus status) {
            this.status = status;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
