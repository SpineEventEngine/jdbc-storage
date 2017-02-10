/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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
import org.spine3.server.aggregate.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.*;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.AggregateRecord.*;

/**
 * Query that inserts a new {@link AggregateStorageRecord} to the {@link Table.AggregateRecord}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class InsertAggregateRecordQuery<I> extends WriteRecordQuery<I, AggregateStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            INSERT_INTO + " %s " + BRACKET_OPEN
                    + ID_COL + COMMA + AGGREGATE_COL + COMMA + SECONDS_COL + COMMA + NANOS_COL + BRACKET_CLOSE
                    + VALUES + Sql.nPlaceholders(4) + SEMICOLON;

    private InsertAggregateRecordQuery(Builder<I> builder) {
        super(builder);
    }


    @Override
    @SuppressWarnings("DuplicateStringLiteralInspection")
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        try {
            final Timestamp timestamp = this.getRecord().getTimestamp();
            statement.setLong(3, timestamp.getSeconds());
            statement.setInt(4, timestamp.getNanos());
            return statement;
        } catch (SQLException e) {
            this.getLogger().error("Failed to prepare statement ", e);
            throw new DatabaseException(e);
        }
    }

    public static <I> Builder <I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setIdIndexInQuery(1)
                .setRecordIndexInQuery(2)
                .setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends WriteRecordQuery.Builder<Builder<I>, InsertAggregateRecordQuery, I, AggregateStorageRecord> {

        @Override
        public InsertAggregateRecordQuery build() {
            return new InsertAggregateRecordQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
