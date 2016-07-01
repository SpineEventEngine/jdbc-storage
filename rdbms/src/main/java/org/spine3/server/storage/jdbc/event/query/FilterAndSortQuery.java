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

package org.spine3.server.storage.jdbc.event.query;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.spine3.protobuf.Messages;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.Query;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import java.sql.PreparedStatement;
import java.util.Iterator;

import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.jdbc.event.query.Constants.*;

public class FilterAndSortQuery extends Query {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String ORDER_BY_TIME_POSTFIX = " ORDER BY " + SECONDS_COL + " ASC, " + NANOSECONDS_COL + " ASC;";

    private final EventStreamQuery streamQuery;

    protected FilterAndSortQuery(Builder builder) {
        super(builder);
        this.streamQuery = builder.streamQuery;
    }

    private static PreparedStatement prepareStatement(ConnectionWrapper connection, EventStreamQuery query) {
        final StringBuilder builder = new StringBuilder(SELECT_EVENT_FROM_TABLE);
        appendTimeConditionSql(builder, query);
        for (EventFilter filter : query.getFilterList()) {
            final String eventType = filter.getEventType();
            if (!eventType.isEmpty()) {
                appendFilterByEventTypeSql(builder, eventType);
            }
            appendFilterByAggregateIdsSql(builder, filter);
        }
        builder.append(ORDER_BY_TIME_POSTFIX);
        final String sql = builder.toString();
        return connection.prepareStatement(sql);
    }

    private static void appendFilterByEventTypeSql(StringBuilder builder, String eventType) {
        appendTo(builder,
                whereOrOr(builder),
                EVENT_TYPE_COL, " = \'", eventType, "\' ");
    }

    private static void appendFilterByAggregateIdsSql(StringBuilder builder, EventFilter filter) {
        for (Any idAny : filter.getAggregateIdList()) {
            final Message aggregateId = Messages.fromAny(idAny);
            final String aggregateIdStr = idToString(aggregateId);
            appendTo(builder,
                    whereOrOr(builder),
                    PRODUCER_ID_COL, " = \'", aggregateIdStr, "\' ");
        }
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static String whereOrOr(StringBuilder builder) {
        final String result = builder.toString().contains("WHERE") ? " OR " : " WHERE ";
        return result;
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static StringBuilder appendTimeConditionSql(StringBuilder builder, EventStreamQuery query) {
        final boolean afterSpecified = query.hasAfter();
        final boolean beforeSpecified = query.hasBefore();
        final String where = " WHERE ";
        if (afterSpecified && !beforeSpecified) {
            builder.append(where);
            appendIsAfterSql(builder, query);
        } else if (!afterSpecified && beforeSpecified) {
            builder.append(where);
            appendIsBeforeSql(builder, query);
        } else if (afterSpecified /* beforeSpecified is true here too */) {
            builder.append(where);
            appendIsBetweenSql(builder, query);
        }
        return builder;
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static StringBuilder appendIsAfterSql(StringBuilder builder, EventStreamQuery query) {
        final Timestamp after = query.getAfter();
        final long seconds = after.getSeconds();
        final int nanos = after.getNanos();
        appendTo(builder, " ",
                SECONDS_COL, " > ", seconds,
                " OR ( ",
                    SECONDS_COL, " = ", seconds, " AND ",
                    NANOSECONDS_COL, " > ", nanos,
                ") ");
        return builder;
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static StringBuilder appendIsBeforeSql(StringBuilder builder, EventStreamQuery query) {
        final Timestamp before = query.getBefore();
        final long seconds = before.getSeconds();
        final int nanos = before.getNanos();
        appendTo(builder, " ",
                SECONDS_COL, " < ", seconds,
                " OR ( ",
                SECONDS_COL, " = ", seconds, " AND ",
                NANOSECONDS_COL, " < ", nanos,
                ") ");
        return builder;
    }

    private static void appendIsBetweenSql(StringBuilder builder, EventStreamQuery query) {
        builder.append(" (");
        appendIsAfterSql(builder, query);
        builder.append(") AND (");
        appendIsBeforeSql(builder, query);
        builder.append(") ");
    }

    private static StringBuilder appendTo(StringBuilder builder, Object... objects) {
        for (Object object : objects) {
            builder.append(object);
        }
        return builder;
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(ORDER_BY_TIME_POSTFIX);
        return builder;
    }

    public Iterator<EventStorageRecord> execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.getConnection(true)) {
            final PreparedStatement statement = prepareStatement(connection, streamQuery);
            return new DbIterator<>(statement, EVENT_COL, EventStorageRecord.getDescriptor());
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends Query.Builder<Builder, FilterAndSortQuery> {

        private EventStreamQuery streamQuery;

        @Override
        public FilterAndSortQuery build() {
            return new FilterAndSortQuery(this);
        }

        public Builder setStreamQuery(EventStreamQuery streamQuery){
            this.streamQuery = streamQuery;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
