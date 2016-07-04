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

import org.spine3.base.CommandStatus;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A base class for working with Command table (Writing case).
 *
 * @author Andrey Lavrov
 */
public class WriteCommandRecordQuery
        extends WriteRecordQuery<String, CommandStorageRecord> {

    private final int statusIndexInQuery;
    private final CommandStatus status;

    protected WriteCommandRecordQuery(Builder<? extends Builder, ? extends WriteCommandRecordQuery> builder) {
        super(builder);
        this.statusIndexInQuery = builder.statusIndexInQuery;
        this.status = builder.status;
    }

    @Override
    @SuppressWarnings("DuplicateStringLiteralInspection")
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        try {
            statement.setString(statusIndexInQuery, status.name());
        } catch (SQLException e) {
            this.getLogger().error("Failed to prepare statement ", e);
            throw new DatabaseException(e);
        }
        return statement;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public abstract static class Builder<B extends Builder<B, Q>, Q extends WriteCommandRecordQuery>
            extends WriteRecordQuery.Builder<B, Q, String, CommandStorageRecord> {

        private int statusIndexInQuery;
        private CommandStatus status;

        public B setStatusIndexInQuery(int statusIndexInQuery) {
            this.statusIndexInQuery = statusIndexInQuery;
            return this.getThis();
        }

        public B setStatus(CommandStatus status) {
            this.status = status;
            return this.getThis();
        }
    }
}
