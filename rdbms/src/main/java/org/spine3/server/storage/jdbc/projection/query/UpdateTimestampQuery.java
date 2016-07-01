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

package org.spine3.server.storage.jdbc.projection.query;

import com.google.protobuf.Timestamp;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.projection.query.Constants.NANOS_COL;
import static org.spine3.server.storage.jdbc.projection.query.Constants.SECONDS_COL;


public class UpdateTimestampQuery extends WriteQuery{

    private final Timestamp timestamp;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String UPDATE_QUERY =
            "UPDATE %s SET " +
                    SECONDS_COL + " = ?, " +
                    NANOS_COL + " = ?;";

    private UpdateTimestampQuery(Builder builder) {
        super(builder);
        this.timestamp = builder.timestamp;
    }

    public static  Builder  newBuilder(String tableName) {
        final Builder builder = new Builder();
        builder.setQuery(format(UPDATE_QUERY, tableName));
        return builder;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        final long seconds = timestamp.getSeconds();
        final int nanos = timestamp.getNanos();
        try {
            statement.setLong(1, seconds);
            statement.setInt(2, nanos);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends WriteQuery.Builder<Builder, UpdateTimestampQuery> {

        private Timestamp timestamp;

        @Override
        public UpdateTimestampQuery build() {
            return new UpdateTimestampQuery(this);
        }

        public Builder setTimestamp(Timestamp timestamp){
            this.timestamp = timestamp;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}