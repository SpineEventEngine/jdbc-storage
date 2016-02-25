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
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
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

    private static final Descriptor RECORD_DESCRIPTOR = AggregateStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    private final IdColumn<Id> idColumn;

    private final InsertQuery insertQuery;
    private final SelectByIdSortedByTimeDescQuery selectByIdSortedByTimeDescQuery;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param aggregateClass the class of aggregates to save to the storage
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ static <ID> JdbcAggregateStorage<ID> newInstance(DataSourceWrapper dataSource,
                                                                 Class<? extends Aggregate<ID, ?>> aggregateClass)
                                                                 throws DatabaseException {
        return new JdbcAggregateStorage<>(dataSource, aggregateClass);
    }

    private JdbcAggregateStorage(DataSourceWrapper dataSource, Class<? extends Aggregate<Id, ?>> aggregateClass)
            throws DatabaseException {
        this.dataSource = dataSource;
        final String tableName = DbTableNameFactory.newTableName(aggregateClass);
        this.insertQuery = new InsertQuery(tableName);
        this.selectByIdSortedByTimeDescQuery = new SelectByIdSortedByTimeDescQuery(tableName);
        this.idColumn = IdColumn.newInstance(aggregateClass);
        new CreateTableIfDoesNotExistQuery().execute(tableName);
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(Id id, AggregateStorageRecord record) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            try (PreparedStatement statement = insertQuery.statement(connection, id, record)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                log().error("Error during writing record, aggregate ID = " + idToString(id), e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
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
            final PreparedStatement statement = selectByIdSortedByTimeDescQuery.statement(connection, id);
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

    private class InsertQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT =
                "INSERT INTO %s " +
                " (" + ID_COL + ", " + AGGREGATE_COL + ", " + SECONDS_COL + ", " + NANOS_COL + ") " +
                " VALUES (?, ?, ?, ?);";

        private final String insertQuery;

        private InsertQuery(String tableName) {
            this.insertQuery = format(INSERT, tableName);
        }

        private PreparedStatement statement(ConnectionWrapper connection, Id id, AggregateStorageRecord record) {
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

    private class CreateTableIfDoesNotExistQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID_COL + " %s, " +
                    AGGREGATE_COL + " BLOB, " +
                    SECONDS_COL + " BIGINT, " +
                    NANOS_COL + " INT " +
                ");";

        private void execute(String tableName) throws DatabaseException {
            final String idColumnType = idColumn.getColumnDataType();
            final String createTableSql = format(CREATE_TABLE_IF_DOES_NOT_EXIST, tableName, idColumnType);
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(createTableSql)) {
                statement.execute();
            } catch (SQLException e) {
                log().error("Error during table creation, table name: " + tableName, e);
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
