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

import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;

import javax.management.Query;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class WriteCommandRecordQuery
        extends WriteRecordQuery<String, CommandStorageRecord> {

    private final int statusIndexInQuery;
    private final int status;

    protected WriteCommandRecordQuery(CommandQueryBuilder<? extends CommandQueryBuilder, ? extends WriteCommandRecordQuery> builder) {
        super(builder);
        this.statusIndexInQuery = builder.statusIndexInQuery;
        this.status = builder.status;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        try {
            statement.setInt(statusIndexInQuery, status);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }

    /**
     */
    public abstract static class CommandQueryBuilder<B extends CommandQueryBuilder<B, Query>, Query extends WriteCommandRecordQuery>
            extends AbstractBuilder<CommandQueryBuilder<B, Query>, Query, String, CommandStorageRecord> {

        private int statusIndexInQuery;
        private int status;

        public CommandQueryBuilder<B, Query> setStatusIndexInQuery(int statusIndexInQuery) {
            this.statusIndexInQuery = statusIndexInQuery;
            return getThis();
        }

        public CommandQueryBuilder<B, Query> setStatus(int status) {
            this.status = status;
            return getThis();
        }

    }
}
