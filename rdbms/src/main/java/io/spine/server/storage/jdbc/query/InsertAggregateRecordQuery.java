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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Timestamp;
import io.spine.core.Event;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.AggregateEventRecordTable;
import io.spine.server.storage.jdbc.AggregateEventRecordTable.Column;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.aggregate;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.id;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.timestamp;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.timestamp_nanos;
import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.version;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.VALUES;
import static io.spine.server.storage.jdbc.Sql.nPlaceholders;
import static io.spine.validate.Validate.isDefault;
import static java.lang.String.format;

/**
 * Query that inserts a new {@link AggregateEventRecord} to the
 * {@link AggregateEventRecordTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class InsertAggregateRecordQuery<I> extends WriteAggregateQuery<I, AggregateEventRecord> {

    private static final String QUERY_TEMPLATE =
            Sql.Query.INSERT_INTO + " %s " + BRACKET_OPEN +
            id + COMMA +
            aggregate + COMMA +
            timestamp + COMMA +
            timestamp_nanos + COMMA +
            version +
            BRACKET_CLOSE
            + VALUES + nPlaceholders(Column.values().length) + SEMICOLON;

    private InsertAggregateRecordQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        try {
            final Timestamp timestamp = getRecord().getTimestamp();

            //TODO:2017-09-11:dmytro.grankin: Remove hard-coded indexes here and in other places.
            // See the related issues: https://github.com/SpineEventEngine/jdbc-storage/issues/36
            statement.setLong(3, timestamp.getSeconds());
            statement.setInt(4, timestamp.getNanos());
            statement.setInt(5, getVersionNumberOfRecord());
            return statement;
        } catch (SQLException e) {
            logFailedToPrepareStatement(e);
            throw new DatabaseException(e);
        }
    }

    private int getVersionNumberOfRecord() {
        final int versionNumber;

        final Event event = getRecord().getEvent();
        if (isDefault(event)) {
            versionNumber = getRecord().getSnapshot()
                                       .getVersion()
                                       .getNumber();
        } else {
            versionNumber = event.getContext()
                                 .getVersion()
                                 .getNumber();
        }
        return versionNumber;
    }

    static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setIdIndexInQuery(1)
               .setRecordIndexInQuery(2)
               .setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends WriteAggregateQuery.Builder<Builder<I>,
                                                                    InsertAggregateRecordQuery,
                                                                    I,
                                                                    AggregateEventRecord> {

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
