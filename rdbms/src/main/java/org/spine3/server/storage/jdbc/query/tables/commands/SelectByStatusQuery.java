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

package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.Internal;
import org.spine3.base.Command;
import org.spine3.base.CommandStatus;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * A query which obtains a {@link Command} by an command status.
 *
 * @author Andrey Lavrov
 */

@Internal
public class  SelectByStatusQuery extends AbstractQuery{
    private final CommandStatus status;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_BY_STATUS_QUERY =
            "SELECT " +  CommandTable.COMMAND_COL + " FROM " + CommandTable.TABLE_NAME +
                    " WHERE " + CommandTable.COMMAND_STATUS_COL + " = ?;";

    /**
     * Creates a new query instance.
     *
     */
    protected SelectByStatusQuery(Builder builder) {
        super(builder);
        this.status = builder.status;
    }

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(SELECT_BY_STATUS_QUERY);
        return builder;
    }

    /**
     * Prepares SQL statement using the connection.
     */
    @Override
    public PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        try {
            statement.setString(1, status.toString());
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }

    public ResultSet execute() throws DatabaseException {
        final ResultSet resultSet;
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final PreparedStatement statement = this.prepareStatement(connection);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return resultSet;
    }

    public static class Builder extends AbstractQuery.Builder<Builder, SelectByStatusQuery> {

        private CommandStatus status;

        @Override
        public SelectByStatusQuery build() {
            return new SelectByStatusQuery(this);
        }

        public Builder setStatus(CommandStatus status){
            this.status = status;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}

