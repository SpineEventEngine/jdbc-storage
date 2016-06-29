package org.spine3.server.storage.jdbc.query.tables.event;


import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.spine3.protobuf.Messages;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.ReadMany;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.EventTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.spine3.base.Identifiers.idToString;

public class FilterAndSortQuery extends AbstractQuery implements ReadMany {
    private static final String ORDER_BY_TIME_POSTFIX = " ORDER BY " + EventTable.SECONDS_COL + " ASC, " + EventTable.NANOSECONDS_COL + " ASC;";

    private final EventStreamQuery streamQuery;

    protected FilterAndSortQuery(Builder builder) {
        super(builder);
        this.streamQuery = builder.streamQuery;
    }

    private static PreparedStatement prepareStatement(ConnectionWrapper connection, EventStreamQuery query) {
        final StringBuilder builder = new StringBuilder(EventTable.SELECT_EVENT_FROM_TABLE);
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
                EventTable.EVENT_TYPE_COL, " = \'", eventType, "\' ");
    }

    private static void appendFilterByAggregateIdsSql(StringBuilder builder, EventFilter filter) {
        for (Any idAny : filter.getAggregateIdList()) {
            final Message aggregateId = Messages.fromAny(idAny);
            final String aggregateIdStr = idToString(aggregateId);
            appendTo(builder,
                    whereOrOr(builder),
                    EventTable.PRODUCER_ID_COL, " = \'", aggregateIdStr, "\' ");
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
                EventTable.SECONDS_COL, " > ", seconds,
                " OR ( ",
                    EventTable.SECONDS_COL, " = ", seconds, " AND ",
                    EventTable.NANOSECONDS_COL, " > ", nanos,
                ") ");
        return builder;
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static StringBuilder appendIsBeforeSql(StringBuilder builder, EventStreamQuery query) {
        final Timestamp before = query.getBefore();
        final long seconds = before.getSeconds();
        final int nanos = before.getNanos();
        appendTo(builder, " ",
                EventTable.SECONDS_COL, " < ", seconds,
                " OR ( ",
                EventTable.SECONDS_COL, " = ", seconds, " AND ",
                EventTable.NANOSECONDS_COL, " < ", nanos,
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

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(ORDER_BY_TIME_POSTFIX);
        return builder;
    }

    @Override
    public ResultSet execute() throws DatabaseException {
        final ResultSet resultSet;
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final PreparedStatement statement = prepareStatement(connection, streamQuery);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return resultSet;
    }

    public static class Builder extends AbstractQuery.Builder<Builder, FilterAndSortQuery> {

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
