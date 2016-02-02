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
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.type.TypeName;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Pattern;

import static org.spine3.protobuf.Messages.fromAny;
import static org.spine3.protobuf.Messages.toAny;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * @param <I> the type of aggregate IDs
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
public class JdbcAggregateStorage<I> extends AggregateStorage<I> {

    /**
     * Aggregate record column name.
     */
    private static final String AGGREGATE = "aggregate";

    /**
     * Aggregate ID column name.
     */
    private static final String ID = "id";

    /**
     * Aggregate event seconds column name.
     */
    private static final String SECONDS = "seconds";

    /**
     * Aggregate event nanoseconds column name.
     */
    private static final String NANOSECONDS = "nanoseconds";

    @SuppressWarnings("UtilityClass")
    private static class SqlDrafts {

        static final String INSERT_RECORD =
                "INSERT INTO %s " +
                        " (" + ID + ", " + AGGREGATE + ", " + SECONDS + ", " + NANOSECONDS + ')' +
                        " VALUES (?, ?, ?, ?);";

        static final String SELECT_BY_ID_SORTED =
                "SELECT " + AGGREGATE + " FROM %s " +
                " WHERE " + ID + " = ? " +
                " ORDER BY " + SECONDS + " DESC, " + NANOSECONDS + " DESC;";

        static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID + " VARCHAR(999), " +
                    AGGREGATE + " BLOB, " +
                    SECONDS + " BIGINT, " +
                    NANOSECONDS + " INT " +
                ");";
    }

    private final DataSourceWrapper dataSource;
    private final String tableName;

    private final Collection<DbIterator> iterators = new HashSet<>();

    private final String insertSql;
    private final String selectAllByIdSql;

    // TODO:2016-01-26:alexander.litus: move to Escaper class
    private static final Pattern PATTERN_DOT = Pattern.compile("\\.");
    private static final Pattern PATTERN_DOLLAR = Pattern.compile("\\$");
    private static final String UNDERSCORE = "_";

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param aggregateClass the class of aggregates to save to the storage
     */
    static <I> JdbcAggregateStorage<I> newInstance(DataSourceWrapper dataSource, Class aggregateClass) {
        return new JdbcAggregateStorage<>(dataSource, aggregateClass);
    }

    private JdbcAggregateStorage(DataSourceWrapper dataSource, Class aggregateClass) {
        this.dataSource = dataSource;
        final String className = aggregateClass.getName();
        final String tableNameTmp = PATTERN_DOT.matcher(className).replaceAll(UNDERSCORE);
        this.tableName = PATTERN_DOLLAR.matcher(tableNameTmp).replaceAll(UNDERSCORE).toLowerCase();

        this.insertSql = setTableName(SqlDrafts.INSERT_RECORD);
        this.selectAllByIdSql = setTableName(SqlDrafts.SELECT_BY_ID_SORTED);
        final String createTableSql = setTableName(SqlDrafts.CREATE_TABLE_IF_DOES_NOT_EXIST);
        createTableIfDoesNotExist(createTableSql, this.tableName);
    }

    private String setTableName(String sql) {
        return String.format(sql, tableName);
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(I id, AggregateStorageRecord record) throws DatabaseException {
        //checkArgument
        final AggregateStorageRecord.Id recordId = AggregateStorage.toRecordId(id);

        final byte[] serializedRecord = serialize(record);
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            insert(connection, recordId, serializedRecord, record);
        }
    }

    private static byte[] serialize(Message message) {
        final Any any = toAny(message);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before {@link Iterator#next()}.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Iterator<AggregateStorageRecord> historyBackward(I id) throws DatabaseException {
        final AggregateStorageRecord.Id recordId = toRecordId(id);
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = selectByIdStatement(connection, recordId)) {
            final DbIterator result = new DbIterator(statement);
            iterators.add(result);
            return result;
        } catch (SQLException e) {
            logTransactionError(recordId, e);
            throw new DatabaseException(e);
        }
    }

    private static class DbIterator implements Iterator<AggregateStorageRecord>, AutoCloseable {

        private final ResultSet resultSet;

        private boolean isHasNextCalledBeforeNext = false;

        private DbIterator(PreparedStatement selectByIdStatement) throws SQLException {
            this.resultSet = selectByIdStatement.executeQuery();
        }

        @Override
        public boolean hasNext() {
            try {
                final boolean hasNext = resultSet.next();
                isHasNextCalledBeforeNext = true;
                return hasNext;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        @Override
        public AggregateStorageRecord next() {
            if (!isHasNextCalledBeforeNext) {
                throw new IllegalStateException("It is required to call hasNext() before next() method.");
            }
            isHasNextCalledBeforeNext = false;
            try {
                final byte[] bytes = resultSet.getBytes(AGGREGATE);
                final AggregateStorageRecord record = toRecord(bytes);
                return record;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing is not supported.");
        }

        @Override
        public void close() throws DatabaseException {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        private static AggregateStorageRecord toRecord(byte[] bytes) {
            final Any.Builder builder = Any.newBuilder();
            builder.setTypeUrl(TypeName.of(AggregateStorageRecord.getDescriptor()).toTypeUrl());
            builder.setValue(ByteString.copyFrom(bytes));
            final AggregateStorageRecord message = fromAny(builder.build());
            return message;
        }
    }

    private void createTableIfDoesNotExist(String sql, String tableName) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            log().error("Error during table creation, table name = " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    private void insert(ConnectionWrapper connection,
                        AggregateStorageRecord.Id id,
                        byte[] serializedAggregate,
                        AggregateStorageRecord record) {
        try (PreparedStatement statement = insertRecordStatement(connection, id, serializedAggregate, record)) {
            statement.execute();
            connection.commit();
        } catch (SQLException e) {
            throw handleDbException(connection, e, id);
        }
    }

    private PreparedStatement insertRecordStatement(ConnectionWrapper connection,
                                                    AggregateStorageRecord.Id id,
                                                    byte[] serializedRecord,
                                                    AggregateStorageRecord record) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(insertSql);
        setId(1, id, statement);
        statement.setBytes(2, serializedRecord);
        final Timestamp timestamp = record.getTimestamp();
        statement.setLong(3, timestamp.getSeconds());
        statement.setInt(4, timestamp.getNanos());
        return statement;
    }

    private PreparedStatement selectByIdStatement(ConnectionWrapper connection,
                                                  AggregateStorageRecord.Id id) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(selectAllByIdSql);
        setId(1, id, statement);
        return statement;
    }

    // TODO:2016-02-01:alexander.litus: find out what IDs to expect on startup
    private static void setId(int idIndex, AggregateStorageRecord.Id id, PreparedStatement statement) throws SQLException {
        final AggregateStorageRecord.Id.TypeCase type = id.getTypeCase();
        switch (type) {
            case STRING_VALUE:
                statement.setString(idIndex, id.getStringValue());
                break;
            case LONG_VALUE:
                statement.setLong(idIndex, id.getLongValue());
                break;
            case INT_VALUE:
                statement.setInt(idIndex, id.getIntValue());
                break;
            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Id type is not set.");
        }
    }

    private static DatabaseException handleDbException(ConnectionWrapper connection, SQLException e, AggregateStorageRecord.Id id) {
        logTransactionError(id, e);
        connection.rollback();
        throw new DatabaseException(e);
    }

    private static void logTransactionError(AggregateStorageRecord.Id id, Exception e) {
        log().error("Error during transaction, aggregate ID = " + id, e);
    }

    @Override
    public void close() throws DatabaseException {
        for (DbIterator iterator : iterators) {
            iterator.close();
        }
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
