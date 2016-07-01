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

package org.spine3.server.storage.jdbc.aggregate.query;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.UpdateRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.aggregate.query.Constants.EVENT_COUNT_COL;
import static org.spine3.server.storage.jdbc.aggregate.query.Constants.ID_COL;


public class InsertEventCountQuery<I> extends UpdateRecordQuery<I> {

    private final int count;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO %s " +
                    " (" + ID_COL + ", " + EVENT_COUNT_COL + ')' +
                    " VALUES (?, ?);";

    private InsertEventCountQuery(Builder<I> builder) {
        super(builder);
        this.count = builder.count;
    }

    @Override
    @SuppressWarnings("DuplicateStringLiteralInspection")
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        try {
            statement.setInt(2, count);
            return statement;
        } catch (SQLException e) {
            this.getLogger().error("Failed to prepare statement ", e);
            throw new DatabaseException(e);
        }
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(INSERT_QUERY, tableName))
                .setIdIndexInQuery(1);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends UpdateRecordQuery.Builder<Builder<I>, InsertEventCountQuery, I> {

        private int count;

        @Override
        public InsertEventCountQuery<I> build() {
            return new InsertEventCountQuery<>(this);
        }

        public Builder<I> setCount(int count) {
            this.count = count;
            return getThis();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
