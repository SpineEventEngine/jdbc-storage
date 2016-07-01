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

import com.google.protobuf.Timestamp;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import static org.spine3.server.storage.jdbc.aggregate.query.Constants.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;


public class InsertRecordQuery<Id> extends WriteRecord<Id, AggregateStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO %s " +
                    " (" + ID_COL + ", " + AGGREGATE_COL + ", " + SECONDS_COL + ", " + NANOS_COL + ") " +
                    " VALUES (?, ?, ?, ?);";

    private InsertRecordQuery(Builder<Id> builder) {
        super(builder);
    }


    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        try {
            final Timestamp timestamp = this.getRecord().getTimestamp();
            statement.setLong(3, timestamp.getSeconds());
            statement.setInt(4, timestamp.getNanos());
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static <Id> Builder <Id> newBuilder(String tableName) {
        final Builder<Id> builder = new Builder<>();
        builder.setIdIndexInQuery(1)
                .setRecordIndexInQuery(2)
                .setQuery(format(INSERT_QUERY, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<Id> extends WriteRecord.Builder<Builder<Id>, InsertRecordQuery, Id, AggregateStorageRecord> {

        @Override
        public InsertRecordQuery build() {
            return new InsertRecordQuery<>(this);
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
