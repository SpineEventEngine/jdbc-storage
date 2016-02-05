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
import org.spine3.server.Identifiers;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newLinkedList;
import static org.spine3.io.IoUtil.closeAll;
import static org.spine3.server.storage.jdbc.util.Serializer.readDeserializedRecord;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the event storage based on the RDBMS.
 *
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcEventStorage extends EventStorage {

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection", "ClassNamingConvention"})
    private static class SQL {

        /**
         * Events table name.
         */
        private static final String TABLE_NAME = "events";

        /**
         * Event ID column name.
         */
        private static final String EVENT_ID = "event_id";

        /**
         * Event record column name.
         */
        private static final String EVENT = "event";

        /**
         * Protobuf type name of the event column name.
         */
        private static final String EVENT_TYPE = "event_type";

        /**
         * Aggregate ID column name.
         */
        private static final String AGGREGATE_ID = "aggregate_id";

        /**
         * Event seconds column name.
         */
        private static final String SECONDS = "seconds";

        /**
         * Event nanoseconds column name.
         */
        private static final String NANOSECONDS = "nanoseconds";

        private static final String SELECT_EVENT_FROM_TABLE = "SELECT " + EVENT + " FROM " + TABLE_NAME + ' ';

        private static class CreateTableIfDoesNotExist {

            private static final String CREATE_TABLE_QUERY =
                    "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                        EVENT_ID + " VARCHAR(512), " +
                        EVENT + " BLOB, " +
                        EVENT_TYPE + " VARCHAR(512), " +
                        AGGREGATE_ID + " VARCHAR(512), " +
                        SECONDS + " BIGINT, " +
                        NANOSECONDS + " INT, " +
                        " PRIMARY KEY(" + EVENT_ID + ')' +
                    ");";

            private static PreparedStatement statement(ConnectionWrapper connection) {
                return connection.prepareStatement(CREATE_TABLE_QUERY);
            }
        }

        private static class Insert {

            private static final String INSERT_QUERY =
                    "INSERT INTO " + TABLE_NAME + " (" +
                        EVENT_ID + ", " +
                        EVENT + ", " +
                        EVENT_TYPE + ", " +
                        AGGREGATE_ID + ", " +
                        SECONDS + ", " +
                        NANOSECONDS +
                    ") VALUES (?, ?, ?, ?, ?, ?);";

            @SuppressWarnings("TypeMayBeWeakened")
            private static PreparedStatement statement(ConnectionWrapper connection, EventStorageRecord record) {
                final PreparedStatement statement = connection.prepareStatement(INSERT_QUERY);
                final byte[] serializedRecord = serialize(record);
                final String eventId = record.getEventId();
                final String eventType = record.getEventType();
                final String aggregateId = record.getAggregateId();
                final Timestamp timestamp = record.getTimestamp();
                final long seconds = timestamp.getSeconds();
                final int nanos = timestamp.getNanos();
                try {
                    statement.setString(1, eventId);
                    statement.setBytes(2, serializedRecord);
                    statement.setString(3, eventType);
                    statement.setString(4, aggregateId);
                    statement.setLong(5, seconds);
                    statement.setInt(6, nanos);
                } catch (SQLException e) {
                    throw new DatabaseException(e);
                }
                return statement;
            }
        }

        private static class SelectEventByEventId {

            private static final String SELECT_QUERY = SELECT_EVENT_FROM_TABLE + " WHERE " + EVENT_ID + " = ?;";

            private static PreparedStatement statement(ConnectionWrapper connection, String id){
                try {
                    final PreparedStatement statement = connection.prepareStatement(SELECT_QUERY);
                    statement.setString(1, id);
                    return statement;
                } catch (SQLException e) {
                    throw new DatabaseException(e);
                }
            }
        }

        private static class FilterAndSort {

            private static final String ORDER_BY_TIME_POSTFIX = " ORDER BY " + SECONDS + " ASC, " + NANOSECONDS + " ASC;";

            private static PreparedStatement statement(ConnectionWrapper connection, EventStreamQuery query) {
                final String sql = buildFilterAndSortSql(query);
                return connection.prepareStatement(sql);
            }

            // TODO:2016-02-04:alexander.litus: add tests for other cases (time, composite filters etc)
            private static String buildFilterAndSortSql(EventStreamQuery query) {
                String result = SELECT_EVENT_FROM_TABLE;
                final String timeConditionQuery = buildTimeConditionSql(query);
                result += timeConditionQuery;
                for (EventFilter filter : query.getFilterList()) {
                    final String eventType = filter.getEventType();
                    if (!eventType.isEmpty()) {
                        final String prefix = result.contains("WHERE") ? " AND " : " WHERE ";
                        final String eventTypeCondition = prefix + EVENT_TYPE + " = \'" + eventType + "\' ";
                        result += eventTypeCondition;
                    }
                    for (Any idAny : filter.getAggregateIdList()) {
                        final Message aggregateId = Messages.fromAny(idAny);
                        final String aggregateIdStr = Identifiers.idToString(aggregateId);
                        final String prefix = result.contains("WHERE") ? " AND " : " WHERE ";
                        final String aggregateIdCondition = prefix + AGGREGATE_ID + " = \'" + aggregateIdStr + "\' ";
                        result += aggregateIdCondition;
                    }
                }
                result += ORDER_BY_TIME_POSTFIX;
                return result;
            }

            private static String buildTimeConditionSql(EventStreamQuery query) {
                final boolean afterSpecified = query.hasAfter();
                final boolean beforeSpecified = query.hasBefore();
                final String where = " WHERE ";
                String result = "";
                if (afterSpecified && !beforeSpecified) {
                    result = where + buildIsAfterSql(query);
                } else if (!afterSpecified && beforeSpecified) {
                    result = where + buildIsBeforeSql(query);
                } else if (afterSpecified /* beforeSpecified is true here too */) {
                    result = where + buildIsBetweenSql(query);
                }
                return result;
            }

            private static String buildIsAfterSql(EventStreamQuery query) {
                final Timestamp after = query.getAfter();
                final long seconds = after.getSeconds();
                final int nanos = after.getNanos();
                final String sql = ' ' +
                        SECONDS + " > " + seconds +
                        " OR ( " +
                        SECONDS + " = " + seconds + " AND " +
                        NANOSECONDS + " > " + nanos +
                        ") ";
                return sql;
            }

            private static String buildIsBeforeSql(EventStreamQuery query) {
                final Timestamp before = query.getBefore();
                final long seconds = before.getSeconds();
                final int nanos = before.getNanos();
                final String sql = ' ' +
                        SECONDS + " < " + seconds +
                        " OR ( " +
                        SECONDS + " = " + seconds + " AND " +
                        NANOSECONDS + " < " + nanos +
                        ") ";
                return sql;
            }

            private static String buildIsBetweenSql(EventStreamQuery query) {
                final String isAfterSql = buildIsAfterSql(query);
                final String isBeforeSql = buildIsBeforeSql(query);
                final String sql = " (" + isAfterSql + ") AND (" + isBeforeSql + ") ";
                return sql;
            }
        }
    }

    private static final Descriptor RECORD_DESCRIPTOR = EventStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     */
    /*package*/ static JdbcEventStorage newInstance(DataSourceWrapper dataSource) {
        return new JdbcEventStorage(dataSource);
    }

    private JdbcEventStorage(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
        createTableIfDoesNotExist();
    }

    private void createTableIfDoesNotExist() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = SQL.CreateTableIfDoesNotExist.statement(connection)) {
            statement.execute();
        } catch (SQLException e) {
            log().error("Error during table creation:", e);
            throw new DatabaseException(e);
        }
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
            final PreparedStatement statement = SQL.FilterAndSort.statement(connection, query);
            final DbIterator<EventStorageRecord> iterator = new DbIterator<>(statement, SQL.EVENT, RECORD_DESCRIPTOR);
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
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            try (PreparedStatement statement = SQL.Insert.statement(connection, record)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                logTransactionError(e, record.getEventId());
                connection.rollback();
                throw new DatabaseException(e);
            }
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
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = SQL.SelectEventByEventId.statement(connection, id)) {
            final EventStorageRecord record = readDeserializedRecord(statement, SQL.EVENT, RECORD_DESCRIPTOR);
            return record;
        } catch (SQLException e) {
            logTransactionError(e, id);
            throw new DatabaseException(e);
        }
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

    static final String DELETE_ALL = "DELETE FROM " + SQL.TABLE_NAME + ";";

    /**
     * Clears all data in the storage.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ void clear() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             final PreparedStatement statement = connection.prepareStatement(DELETE_ALL)) {
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static void logTransactionError(SQLException e, String id) {
        log().error("Error during transaction, event ID = " + id, e);
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
