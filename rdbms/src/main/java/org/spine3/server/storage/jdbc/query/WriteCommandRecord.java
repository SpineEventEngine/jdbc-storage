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

package org.spine3.server.storage.jdbc.query;

import org.spine3.base.CommandStatus;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.WriteCommandRecordQuery;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A base class for working with Command table (Writing case).
 *
 * @author Andrey Lavrov
 */

public class WriteCommandRecord extends WriteRecord<String, CommandStorageRecord> {

    private final CommandStatus status;
    private final int statusIndexInQuery;

    /**
     * Creates a new query instance based on the passed builder.
     */
    protected WriteCommandRecord(Builder builder) {
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


    public static class Builder extends WriteRecord.Builder <Builder, WriteCommandRecord, String, CommandStorageRecord> {

        private int statusIndexInQuery;
        private CommandStatus status;

        public Builder setStatusIndexInQuery(int statusIndexInQuery) {
            this.statusIndexInQuery = statusIndexInQuery;
            return getThis();
        }

        public Builder setStatus(CommandStatus status) {
            this.status = status;
            return getThis();
        }

        @Override
        public Query build() {
            return new WriteCommandRecord(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
