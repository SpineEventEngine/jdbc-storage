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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.util.*;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static java.lang.String.format;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.io.IoUtil.closeAll;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * @param <Id> the type of aggregate IDs
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcAggregateStorage<Id> extends AggregateStorage<Id> {

    /**
     * Aggregate ID column name.
     */
    private static final String ID_COL = "id";

    /**
     * Aggregate record column name.
     */
    private static final String AGGREGATE_COL = "aggregate";

    /**
     * Aggregate event seconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SECONDS_COL = "seconds";

    /**
     * Aggregate event nanoseconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String NANOS_COL = "nanoseconds";

    /**
     * A count of events after the last snapshot column name.
     */
    private static final String EVENT_COUNT = "event_count";

    /**
     * A suffix of a table name where the last event time is stored.
     */
    private static final String EVENT_COUNT_TABLE_NAME_SUFFIX = "_event_count";

    private static final Descriptor RECORD_DESCRIPTOR = AggregateStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    private final IdColumn<Id> idColumn;

    private final String mainTableName;
    private final String eventCountTableName;

    private final SelectByIdSortedByTimeDescQuery selectByIdSortedQuery;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param aggregateClass the class of aggregates to save to the storage
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ static <ID> JdbcAggregateStorage<ID> newInstance(DataSourceWrapper dataSource,
                                                                 Class<? extends Aggregate<ID, ?, ?>> aggregateClass)
                                                                 throws DatabaseException {
        return new JdbcAggregateStorage<>(dataSource, aggregateClass);
    }

    private JdbcAggregateStorage(DataSourceWrapper dataSource, Class<? extends Aggregate<Id, ?, ?>> aggregateClass)
            throws DatabaseException {
        this.dataSource = dataSource;
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.selectByIdSortedQuery = new SelectByIdSortedByTimeDescQuery(mainTableName);
        new CreateMainTableIfDoesNotExistQuery().execute(mainTableName);
        new CreateEventCountTableIfDoesNotExistQuery().execute(eventCountTableName);
    }

    @Override
    public int readEventCountAfterLastSnapshot() {
        final Integer count = new SelectEventCountQuery().execute();
        if (count == null) {
            return 0;
        }
        return count;
    }

    @Override
    public void writeEventCountAfterLastSnapshot(int count) {
        if (containsEventCount()) {
            new UpdateEventCountQuery(count).execute();
        } else {
            new InsertEventCountQuery(count).execute();
        }
    }

    private boolean containsEventCount() {
        final Integer count = new SelectEventCountQuery().execute();
        final boolean contains = count != null;
        return contains;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(Id id, AggregateStorageRecord record) throws DatabaseException {
        new InsertRecordQuery(id, record).execute();
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before {@link Iterator#next()}.
     *
     * @return a new {@link DbIterator} instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Iterator<AggregateStorageRecord> historyBackward(Id id) throws DatabaseException {
        checkNotNull(id);
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final PreparedStatement statement = selectByIdSortedQuery.statement(connection, id);
            final DbIterator<AggregateStorageRecord> iterator = new DbIterator<>(statement, AGGREGATE_COL, RECORD_DESCRIPTOR);
            iterators.add(iterator);
            return iterator;
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

    private class InsertRecordQuery extends WriteQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT =
                "INSERT INTO %s " +
                " (" + ID_COL + ", " + AGGREGATE_COL + ", " + SECONDS_COL + ", " + NANOS_COL + ") " +
                " VALUES (?, ?, ?, ?);";

        private final String insertQuery;
        private final AggregateStorageRecord record;
        private final Id id;

        private InsertRecordQuery(Id id, AggregateStorageRecord record) {
            super(dataSource);
            this.insertQuery = format(INSERT, mainTableName);
            this.record = record;
            this.id = id;
        }

        @Override
        protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(insertQuery);
            final byte[] serializedRecord = serialize(record);
            try {
                idColumn.setId(1, id, statement);
                statement.setBytes(2, serializedRecord);
                final Timestamp timestamp = record.getTimestamp();
                statement.setLong(3, timestamp.getSeconds());
                statement.setInt(4, timestamp.getNanos());
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
            return statement;
        }

        @Override
        protected void logError(SQLException exception) {
            log().error("Error during writing record, aggregate ID = " + idToString(id), exception);
        }
    }

    private class SelectByIdSortedByTimeDescQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_BY_ID_SORTED_BY_TIME_DESC =
                "SELECT " + AGGREGATE_COL + " FROM %s " +
                " WHERE " + ID_COL + " = ? " +
                " ORDER BY " + SECONDS_COL + " DESC, " + NANOS_COL + " DESC;";

        private final String query;

        private SelectByIdSortedByTimeDescQuery(String tableName) {
            this.query = format(SELECT_BY_ID_SORTED_BY_TIME_DESC, tableName);
        }

        private PreparedStatement statement(ConnectionWrapper connection, Id id) {
            final PreparedStatement statement = connection.prepareStatement(query);
            idColumn.setId(1, id, statement);
            return statement;
        }
    }

    private abstract class CreateTableIfDoesNotExistQuery {

        private final String query;

        protected CreateTableIfDoesNotExistQuery(String query) {
            this.query = query;
        }

        protected void execute(String tableName) throws DatabaseException {
            final String idColumnType = idColumn.getColumnDataType();
            final String createTableSql = format(query, tableName, idColumnType);
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(createTableSql)) {
                statement.execute();
            } catch (SQLException e) {
                log().error("Error during table creation, table name: " + tableName, e);
                throw new DatabaseException(e);
            }
        }
    }

    private class CreateMainTableIfDoesNotExistQuery extends CreateTableIfDoesNotExistQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String QUERY =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID_COL + " %s, " +
                    AGGREGATE_COL + " BLOB, " +
                    SECONDS_COL + " BIGINT, " +
                    NANOS_COL + " INT " +
                ");";


        protected CreateMainTableIfDoesNotExistQuery() {
            super(QUERY);
        }
    }

    private class CreateEventCountTableIfDoesNotExistQuery extends CreateTableIfDoesNotExistQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String QUERY =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    EVENT_COUNT + " BIGINT " +
                ");";

        protected CreateEventCountTableIfDoesNotExistQuery() {
            super(QUERY);
        }
    }

    private class WriteEventCountQuery extends WriteQuery {

        private final String query;
        private final int count;

        private WriteEventCountQuery(String query, int count) {
            super(dataSource);
            this.query = query;
            this.count = count;
        }

        @Override
        protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(query);
            try {
                statement.setLong(1, count);
                return statement;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        @Override
        protected void logError(SQLException e) {
            log().error("Failed to write the count of events after the last snapshot.", e);
        }
    }

    private class InsertEventCountQuery extends WriteEventCountQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT_QUERY =
                "INSERT INTO %s " +
                " (" + EVENT_COUNT + ')' +
                " VALUES (?);";

        private InsertEventCountQuery(int count) {
            super(format(INSERT_QUERY, eventCountTableName), count);
        }
    }

    private class UpdateEventCountQuery extends WriteEventCountQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String UPDATE_QUERY = "UPDATE %s SET " + EVENT_COUNT + " = ?;";

        private UpdateEventCountQuery(int count) {
            super(format(UPDATE_QUERY, eventCountTableName), count);
        }
    }

    private class SelectEventCountQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_QUERY = "SELECT " + EVENT_COUNT + " FROM %s ;";

        private final String selectQuery;

        private SelectEventCountQuery() {
            this.selectQuery = format(SELECT_QUERY, eventCountTableName);
        }

        @Nullable
        private Integer execute() throws DatabaseException {
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(selectQuery);
                 ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                final int eventCount = resultSet.getInt(EVENT_COUNT);
                return eventCount;
            } catch (SQLException e) {
                log().error("Failed to read an event count after the last snapshot.", e);
                throw new DatabaseException(e);
            }
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcAggregateStorage.class);
    }
}
