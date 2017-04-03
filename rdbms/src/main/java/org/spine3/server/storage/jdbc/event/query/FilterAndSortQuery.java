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

package org.spine3.server.storage.jdbc.event.query;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.spine3.base.Event;
import org.spine3.protobuf.AnyPacker;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.table.EventTable;
import org.spine3.server.storage.jdbc.table.EventTable.Column;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import java.sql.PreparedStatement;
import java.util.Iterator;

import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.GT;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.LT;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.AND;
import static org.spine3.server.storage.jdbc.Sql.Query.ASC;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.OR;
import static org.spine3.server.storage.jdbc.Sql.Query.ORDER_BY;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.table.EventTable.Column.event;
import static org.spine3.server.storage.jdbc.table.EventTable.Column.nanoseconds;
import static org.spine3.server.storage.jdbc.table.EventTable.Column.seconds;

/**
 * Query that selects {@linkplain Event events} by specified {@link EventStreamQuery}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class FilterAndSortQuery extends StorageQuery {

    private static final String SELECT_EVENT_FROM_TABLE = SELECT + event.name() +
                                                          FROM + EventTable.TABLE_NAME;
    private static final String QUERY_TEMPLATE = ORDER_BY.toString() + seconds + ASC + COMMA
                                                 + nanoseconds + ASC + SEMICOLON;
    private static final String ESCAPED_EQUAL_START = " = \'";
    private static final String ESCAPED_EQUAL_END = "\' ";

    private final EventStreamQuery streamQuery;

    protected FilterAndSortQuery(Builder builder) {
        super(builder);
        this.streamQuery = builder.streamQuery;
    }

    private static PreparedStatement prepareStatement(ConnectionWrapper connection,
                                                      EventStreamQuery query) {
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
                 Column.event_type.name(), ESCAPED_EQUAL_START, eventType, ESCAPED_EQUAL_END);
    }

    private static void appendFilterByAggregateIdsSql(StringBuilder builder, EventFilter filter) {
        for (Any idAny : filter.getAggregateIdList()) {
            final Message aggregateId = AnyPacker.unpack(idAny);
            final String aggregateIdStr = idToString(aggregateId);
            appendTo(builder,
                     whereOrOr(builder),
                     Column.producer_id.name(),
                     ESCAPED_EQUAL_START, aggregateIdStr, ESCAPED_EQUAL_END);
        }
    }

    private static String whereOrOr(StringBuilder builder) {
        final String result = builder.indexOf(WHERE.toString()
                                                   .trim()) >= 0
                              ? OR.toString()
                              : WHERE.toString();
        return result;
    }

    private static StringBuilder appendTimeConditionSql(StringBuilder builder,
                                                        EventStreamQuery query) {
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

    private static StringBuilder appendIsAfterSql(StringBuilder builder, EventStreamQuery query) {
        final Timestamp after = query.getAfter();
        final long seconds = after.getSeconds();
        final int nanos = after.getNanos();
        appendTo(builder, ' ',
                 Column.seconds.name(), GT, seconds,
                 OR, BRACKET_OPEN,
                 Column.seconds.name(), EQUAL, seconds, AND,
                 Column.nanoseconds.name(), GT, nanos,
                 BRACKET_CLOSE.toString());
        return builder;
    }

    private static StringBuilder appendIsBeforeSql(StringBuilder builder, EventStreamQuery query) {
        final Timestamp before = query.getBefore();
        final long seconds = before.getSeconds();
        final int nanos = before.getNanos();
        appendTo(builder, ' ',
                 Column.seconds.name(), LT, seconds,
                 OR, BRACKET_OPEN,
                 Column.seconds.name(), EQUAL, seconds, AND,
                 Column.nanoseconds.name(), LT, nanos,
                 BRACKET_CLOSE);
        return builder;
    }

    private static void appendIsBetweenSql(StringBuilder builder, EventStreamQuery query) {
        builder.append(BRACKET_OPEN);
        appendIsAfterSql(builder, query);
        builder.append(BRACKET_CLOSE)
               .append(AND)
               .append(BRACKET_OPEN);
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

    public Iterator<Event> execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.getConnection(true)) {
            final PreparedStatement statement = prepareStatement(connection, streamQuery);
            return new DbIterator<>(statement, Column.event.name(), Event.getDescriptor());
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends StorageQuery.Builder<Builder, FilterAndSortQuery> {

        private EventStreamQuery streamQuery;

        @Override
        public FilterAndSortQuery build() {
            return new FilterAndSortQuery(this);
        }

        public Builder setStreamQuery(EventStreamQuery streamQuery) {
            this.streamQuery = streamQuery;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
