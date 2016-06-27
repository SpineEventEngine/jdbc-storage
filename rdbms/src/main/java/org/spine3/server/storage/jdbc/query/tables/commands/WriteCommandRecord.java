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

import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A base class for working with Command table (Writing case).
 *
 * @author Andrey Lavrov
 */

public abstract class WriteCommandRecord
        extends WriteRecord<String, CommandStorageRecord> {

    private int statusIndexInQuery;
    private final CommandStatus status;

    protected WriteCommandRecord(Builder<? extends Builder, ? extends WriteCommandRecord> builder) {
        super(builder);
        this.statusIndexInQuery = builder.statusIndexInQuery;
        this.status = builder.status;
    }

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

    public abstract static class Builder<B extends Builder<B, Q>, Q extends WriteCommandRecord>
            extends WriteRecord.Builder<B, Q, String, CommandStorageRecord> {

        private int statusIndexInQuery;
        private CommandStatus status;

        public Builder<B, Q> setStatusIndexInQuery(int statusIndexInQuery) {
            this.statusIndexInQuery = statusIndexInQuery;
            return this.getThis();
        }

        public Builder<B, Q> setStatus(CommandStatus status) {
            this.status = status;
            return this.getThis();
        }

    }
}