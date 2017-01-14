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
import org.spine3.protobuf.AnyPacker;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import java.sql.PreparedStatement;
import java.util.Iterator;

import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.jdbc.Sql.Common.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.Common.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.Common.COMMA;
import static org.spine3.server.storage.jdbc.Sql.Common.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.Common.GT;
import static org.spine3.server.storage.jdbc.Sql.Common.LT;
import static org.spine3.server.storage.jdbc.Sql.Common.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.AND;
import static org.spine3.server.storage.jdbc.Sql.Query.ASC;
import static org.spine3.server.storage.jdbc.Sql.Query.OR;
import static org.spine3.server.storage.jdbc.Sql.Query.ORDER_BY;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_TYPE_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.NANOSECONDS_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.PRODUCER_ID_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.SECONDS_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.SELECT_EVENT_FROM_TABLE;

/**
 * Query that selects {@link EventStorageRecord} by specified {@link EventStreamQuery}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class FilterAndSortQuery extends StorageQuery {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE = ORDER_BY + SECONDS_COL + ASC + COMMA
            + NANOSECONDS_COL + ASC + SEMICOLON;
    private static final String ESCAPED_EQUAL_START = " = \'";
    private static final String ESCAPED_EQUAL_END = "\' ";

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
        builder.append(QUERY_TEMPLATE);
        final String sql = builder.toString();
        return connection.prepareStatement(sql);
    }

    private static void appendFilterByEventTypeSql(StringBuilder builder, String eventType) {
        appendTo(builder,
                whereOrOr(builder),
                EVENT_TYPE_COL, ESCAPED_EQUAL_START, eventType, ESCAPED_EQUAL_END);
    }

    private static void appendFilterByAggregateIdsSql(StringBuilder builder, EventFilter filter) {
        for (Any idAny : filter.getAggregateIdList()) {
            final Message aggregateId = AnyPacker.unpack(idAny);
            final String aggregateIdStr = idToString(aggregateId);
            appendTo(builder,
                    whereOrOr(builder),
                    PRODUCER_ID_COL, ESCAPED_EQUAL_START, aggregateIdStr, ESCAPED_EQUAL_END);
        }
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static String whereOrOr(StringBuilder builder) {
        final String result = builder.toString().contains(WHERE.toString().trim())
                                                        ? OR.toString()
                                                        : WHERE.toString();
        return result;
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static StringBuilder appendTimeConditionSql(StringBuilder builder, EventStreamQuery query) {
        final boolean afterSpecified = query.hasAfter();
        final boolean beforeSpecified = query.hasBefore();
        final String where = WHERE.toString();
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
        appendTo(builder, ' ',
                SECONDS_COL, GT, seconds,
                OR, BRACKET_OPEN,
                    SECONDS_COL, EQUAL, seconds, AND,
                    NANOSECONDS_COL, GT, nanos,
                BRACKET_CLOSE.toString());
        return builder;
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static StringBuilder appendIsBeforeSql(StringBuilder builder, EventStreamQuery query) {
        final Timestamp before = query.getBefore();
        final long seconds = before.getSeconds();
        final int nanos = before.getNanos();
        appendTo(builder, ' ',
                SECONDS_COL, LT, seconds,
                OR, BRACKET_OPEN,
                SECONDS_COL, EQUAL, seconds, AND,
                NANOSECONDS_COL, LT, nanos,
                BRACKET_CLOSE);
        return builder;
    }

    private static void appendIsBetweenSql(StringBuilder builder, EventStreamQuery query) {
        builder.append(BRACKET_OPEN);
        appendIsAfterSql(builder, query);
        builder.append(BRACKET_CLOSE).append(AND).append(BRACKET_OPEN);
        appendIsBeforeSql(builder, query);
        builder.append(BRACKET_CLOSE);
    }

    private static StringBuilder appendTo(StringBuilder builder, Object... objects) {
        for (Object object : objects) {
            builder.append(object);
        }
        return builder;
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(QUERY_TEMPLATE);
        return builder;
    }

    public Iterator<EventStorageRecord> execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.getConnection(true)) {
            final PreparedStatement statement = prepareStatement(connection, streamQuery);
            return new DbIterator<>(statement, EVENT_COL, EventStorageRecord.getDescriptor());
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends StorageQuery.Builder<Builder, FilterAndSortQuery> {

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
