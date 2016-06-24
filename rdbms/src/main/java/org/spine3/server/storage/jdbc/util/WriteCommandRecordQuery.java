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

package org.spine3.server.storage.jdbc.util;

import org.spine3.base.CommandStatus;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A base class for working with Command table (Writing case).
 *
 * @author Andrey Lavrov
 */

public abstract class WriteCommandRecordQuery
        extends WriteRecordQuery<String, CommandStorageRecord> {

    private final int statusIndexInQuery;
    private final CommandStatus status;

    /**
     * Creates a new query instance based on the passed builder.
     */
    protected WriteCommandRecordQuery(CommandQueryBuilder<? extends CommandQueryBuilder, ? extends WriteCommandRecordQuery> builder) {
        super(builder);
        this.statusIndexInQuery = builder.statusIndexInQuery;
        this.status = builder.status;
    }

    /**
     * Prepares SQL statement using the connection.
     */
    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        try {
            statement.setString(statusIndexInQuery, status.name());
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }

    /**
     * Abstract builder extension to match additional columns in commands table.
     */
    public abstract static class CommandQueryBuilder<Builder extends CommandQueryBuilder<Builder, Query>, Query extends WriteCommandRecordQuery>
            extends AbstractBuilder<CommandQueryBuilder<Builder, Query>, Query, String, CommandStorageRecord> {

        private int statusIndexInQuery;
        private CommandStatus status;

        public CommandQueryBuilder<Builder, Query> setStatusIndexInQuery(int statusIndexInQuery) {
            this.statusIndexInQuery = statusIndexInQuery;
            return getThis();
        }

        public CommandQueryBuilder<Builder, Query> setStatus(CommandStatus status) {
            this.status = status;
            return getThis();
        }

    }
}
