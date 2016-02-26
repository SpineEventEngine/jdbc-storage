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

package org.spine3.server.storage.jdbc;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.protobuf.Messages;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;
import org.spine3.server.storage.jdbc.util.IdColumn.StringIdColumn;
import org.spine3.server.storage.jdbc.util.SelectByIdQuery;
import org.spine3.server.storage.jdbc.util.WriteQuery;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newLinkedList;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.io.IoUtil.closeAll;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the event storage based on the RDBMS.
 *
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcEventStorage extends EventStorage {

    /**
     * Events table name.
     */
    private static final String TABLE_NAME = "events";

    /**
     * Event ID column name.
     */
    private static final String EVENT_ID_COL = "event_id";

    /**
     * Event record column name.
     */
    private static final String EVENT_COL = "event";

    /**
     * Protobuf type name of the event column name.
     */
    private static final String EVENT_TYPE_COL = "event_type";

    /**
     * Producer ID column name.
     */
    private static final String PRODUCER_ID_COL = "producer_id";

    /**
     * Event seconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SECONDS_COL = "seconds";

    /**
     * Event nanoseconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String NANOSECONDS_COL = "nanoseconds";

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_EVENT_FROM_TABLE = "SELECT " + EVENT_COL + " FROM " + TABLE_NAME + ' ';

    private static final Descriptor RECORD_DESCRIPTOR = EventStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    /**
     * Creates a new storage instance.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     * @param dataSource the dataSource wrapper
     */
    /*package*/ static JdbcEventStorage newInstance(DataSourceWrapper dataSource) throws DatabaseException {
        return new JdbcEventStorage(dataSource);
    }

    private JdbcEventStorage(DataSourceWrapper dataSource) throws DatabaseException {
        this.dataSource = dataSource;
        CreateTableIfDoesNotExistQuery.execute(dataSource);
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before {@link Iterator#next()}.
     *
     * @return a wrapped {@link DbIterator} instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public Iterator<Event> iterator(EventStreamQuery query) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final PreparedStatement statement = FilterAndSortQuery.prepareStatement(connection, query);
            final DbIterator<EventStorageRecord> iterator = new DbIterator<>(statement, EVENT_COL, RECORD_DESCRIPTOR);
            iterators.add(iterator);
            final Iterator<Event> result = toEventIterator(iterator);
            return result;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(EventStorageRecord record) throws DatabaseException {
        if (containsRecord(record.getEventId())) {
            new UpdateQuery(record).execute();
        } else {
            new InsertQuery(record).execute();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    protected EventStorageRecord readInternal(EventId eventId) throws DatabaseException {
        final String id = eventId.getUuid();
        final EventStorageRecord record = new SelectEventByIdQuery().execute(id);
        return record;
    }

    private boolean containsRecord(String id) {
        final EventStorageRecord record = new SelectEventByIdQuery().execute(id);
        final boolean contains = record != null;
        return contains;
    }

    @Override
    public void close() throws DatabaseException {
        closeAll(iterators);
        iterators.clear();
        dataSource.close();
        try {
            super.close();
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    @SuppressWarnings("UtilityClass")
    private static class CreateTableIfDoesNotExistQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String CREATE_TABLE_QUERY =
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                    EVENT_ID_COL + " VARCHAR(512), " +
                    EVENT_COL + " BLOB, " +
                    EVENT_TYPE_COL + " VARCHAR(512), " +
                    PRODUCER_ID_COL + " VARCHAR(512), " +
                    SECONDS_COL + " BIGINT, " +
                    NANOSECONDS_COL + " INT, " +
                    " PRIMARY KEY(" + EVENT_ID_COL + ')' +
                ");";

        private static void execute(DataSourceWrapper dataSource) throws DatabaseException {
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(CREATE_TABLE_QUERY)) {
                statement.execute();
            } catch (SQLException e) {
                log().error("Error during table creation:", e);
                throw new DatabaseException(e);
            }
        }
    }

    private class InsertQuery extends WriteQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT_QUERY =
                "INSERT INTO " + TABLE_NAME + " (" +
                    EVENT_ID_COL + ", " +
                    EVENT_COL + ", " +
                    EVENT_TYPE_COL + ", " +
                    PRODUCER_ID_COL + ", " +
                    SECONDS_COL + ", " +
                    NANOSECONDS_COL +
                ") VALUES (?, ?, ?, ?, ?, ?);";

        private final EventStorageRecord record;

        private InsertQuery(EventStorageRecord record) {
            super(dataSource);
            this.record = record;
        }

        @Override
        protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(INSERT_QUERY);
            final Timestamp timestamp = record.getTimestamp();
            try {
                final String eventId = record.getEventId();
                statement.setString(1, eventId);

                final byte[] serializedRecord = serialize(record);
                statement.setBytes(2, serializedRecord);

                final String eventType = record.getEventType();
                statement.setString(3, eventType);

                final String producerId = record.getProducerId();
                statement.setString(4, producerId);

                final long seconds = timestamp.getSeconds();
                statement.setLong(5, seconds);

                final int nanos = timestamp.getNanos();
                statement.setInt(6, nanos);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
            return statement;
        }

        @Override
        protected void logError(SQLException exception) {
            log().error("Failed to insert event record, event ID: {}", record.getEventId());
        }
    }

    private class UpdateQuery extends WriteQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " +
                    EVENT_COL + " = ?, " +
                    EVENT_TYPE_COL + " = ?, " +
                    PRODUCER_ID_COL + " = ?, " +
                    SECONDS_COL + " = ?, " +
                    NANOSECONDS_COL + " = ? " +
                " WHERE " + EVENT_ID_COL + " = ? ;";

        private final EventStorageRecord record;

        private UpdateQuery(EventStorageRecord record) {
            super(dataSource);
            this.record = record;
        }

        @Override
        protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(INSERT_QUERY);
            final Timestamp timestamp = record.getTimestamp();
            try {
                final byte[] serializedRecord = serialize(record);
                statement.setBytes(1, serializedRecord);

                final String eventType = record.getEventType();
                statement.setString(2, eventType);

                final String producerId = record.getProducerId();
                statement.setString(3, producerId);

                final long seconds = timestamp.getSeconds();
                statement.setLong(4, seconds);

                final int nanos = timestamp.getNanos();
                statement.setInt(5, nanos);

                final String eventId = record.getEventId();
                statement.setString(6, eventId);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
            return statement;
        }

        @Override
        protected void logError(SQLException exception) {
            log().error("Failed to update event record, event ID: {}", record.getEventId());
        }
    }

    private class SelectEventByIdQuery extends SelectByIdQuery<String, EventStorageRecord> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_QUERY = SELECT_EVENT_FROM_TABLE + " WHERE " + EVENT_ID_COL + " = ?;";

        private SelectEventByIdQuery() {
            super(SELECT_QUERY, dataSource, new StringIdColumn());
            setMessageColumnName(EVENT_COL);
            setMessageDescriptor(RECORD_DESCRIPTOR);
        }
    }

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection"})
    private static class FilterAndSortQuery {

        private static final String ORDER_BY_TIME_POSTFIX = " ORDER BY " + SECONDS_COL + " ASC, " + NANOSECONDS_COL + " ASC;";

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
                    whereOrAnd(builder),
                    EVENT_TYPE_COL, " = \'", eventType, "\' ");
        }

        private static void appendFilterByAggregateIdsSql(StringBuilder builder, EventFilter filter) {
            for (Any idAny : filter.getAggregateIdList()) {
                final Message aggregateId = Messages.fromAny(idAny);
                final String aggregateIdStr = idToString(aggregateId);
                appendTo(builder,
                        whereOrAnd(builder),
                        PRODUCER_ID_COL, " = \'", aggregateIdStr, "\' ");
            }
        }

        private static String whereOrAnd(StringBuilder builder) {
            final String result = builder.toString().contains("WHERE") ? " AND " : " WHERE ";
            return result;
        }

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
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcEventStorage.class);
    }
}
