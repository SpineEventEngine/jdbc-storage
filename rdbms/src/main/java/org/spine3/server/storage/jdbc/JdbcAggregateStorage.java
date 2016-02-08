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
import org.spine3.server.EntityId;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.util.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static java.lang.String.format;
import static org.spine3.io.IoUtil.closeAll;
import static org.spine3.server.Identifiers.idToString;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * @param <ID> the type of aggregate IDs. See {@link EntityId} for details.
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcAggregateStorage<ID> extends AggregateStorage<ID> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private interface SQL {

        /**
         * Aggregate ID column name.
         */
        String ID = "id";

        /**
         * Aggregate record column name.
         */
        String AGGREGATE = "aggregate";

        /**
         * Aggregate event seconds column name.
         */
        String SECONDS = "seconds";

        /**
         * Aggregate event nanoseconds column name.
         */
        String NANOSECONDS = "nanoseconds";

        String INSERT =
                "INSERT INTO %s " +
                " (" + ID + ", " + AGGREGATE + ", " + SECONDS + ", " + NANOSECONDS + ") " +
                " VALUES (?, ?, ?, ?);";

        String SELECT_BY_ID_SORTED_BY_TIME_DESC =
                "SELECT " + AGGREGATE + " FROM %s " +
                " WHERE " + ID + " = ? " +
                " ORDER BY " + SECONDS + " DESC, " + NANOSECONDS + " DESC;";

        String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID + " %s, " +
                    AGGREGATE + " BLOB, " +
                    SECONDS + " BIGINT, " +
                    NANOSECONDS + " INT " +
                ");";
    }

    private static final Descriptor RECORD_DESCRIPTOR = AggregateStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    private final IdHelper<ID> idHelper;

    /**
     * SQL queries.
     */
    private final String insertQuery;
    private final String selectByIdSortedByTimeDescQuery;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param aggregateClass the class of aggregates to save to the storage
     */
    /*package*/ static <ID> JdbcAggregateStorage<ID> newInstance(DataSourceWrapper dataSource,
                                                     Class<? extends Aggregate<ID, ?>> aggregateClass) {
        return new JdbcAggregateStorage<>(dataSource, aggregateClass);
    }

    private JdbcAggregateStorage(DataSourceWrapper dataSource, Class<? extends Aggregate<ID, ?>> aggregateClass) {
        this.dataSource = dataSource;

        final String tableName = DbTableNameFactory.newTableName(aggregateClass);
        this.insertQuery = format(SQL.INSERT, tableName);
        this.selectByIdSortedByTimeDescQuery = format(SQL.SELECT_BY_ID_SORTED_BY_TIME_DESC, tableName);

        this.idHelper = IdHelper.newInstance(aggregateClass);
        createTableIfDoesNotExist(tableName);
    }

    private void createTableIfDoesNotExist(String tableName) throws DatabaseException {
        final String idColumnType = idHelper.getIdColumnType();
        final String createTableSql = format(SQL.CREATE_TABLE_IF_DOES_NOT_EXIST, tableName, idColumnType);
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(createTableSql)) {
            statement.execute();
        } catch (SQLException e) {
            log().error("Error during table creation, table name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(ID id, AggregateStorageRecord record) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            try (PreparedStatement statement = insertRecordStatement(connection, id, record)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                log().error("Error during transaction, aggregate ID = " + idToString(id), e);
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
     * @return a wrapped {@link DbIterator} instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Iterator<AggregateStorageRecord> historyBackward(ID id) throws DatabaseException {
        checkNotNull(id);
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final PreparedStatement statement = selectByIdStatement(connection, id);
            final DbIterator<AggregateStorageRecord> iterator = new DbIterator<>(statement, SQL.AGGREGATE, RECORD_DESCRIPTOR);
            iterators.add(iterator);
            return iterator;
        }
    }

    private PreparedStatement insertRecordStatement(ConnectionWrapper connection, ID id, AggregateStorageRecord record) {
        final PreparedStatement statement = connection.prepareStatement(insertQuery);
        final byte[] serializedRecord = serialize(record);
        try {
            idHelper.setId(1, id, statement);
            statement.setBytes(2, serializedRecord);
            final Timestamp timestamp = record.getTimestamp();
            statement.setLong(3, timestamp.getSeconds());
            statement.setInt(4, timestamp.getNanos());
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }

    private PreparedStatement selectByIdStatement(ConnectionWrapper connection, ID id) {
        final PreparedStatement statement = connection.prepareStatement(selectByIdSortedByTimeDescQuery);
        idHelper.setId(1, id, statement);
        return statement;
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

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcAggregateStorage.class);
    }
}
