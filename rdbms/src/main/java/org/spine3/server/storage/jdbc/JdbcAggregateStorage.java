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
import org.spine3.server.EntityId;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.reflect.Classes;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.io.IoUtil.closeAll;
import static org.spine3.protobuf.Messages.fromAny;
import static org.spine3.protobuf.Messages.toAny;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * @param <I> the type of aggregate IDs. See {@link EntityId} for details.
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
public class JdbcAggregateStorage<I> extends AggregateStorage<I> {

    /**
     * Aggregate ID column name.
     */
    private static final String ID = "id";

    /**
     * Aggregate record column name.
     */
    private static final String AGGREGATE = "aggregate";

    /**
     * Aggregate event seconds column name.
     */
    private static final String SECONDS = "seconds";

    /**
     * Aggregate event nanoseconds column name.
     */
    private static final String NANOSECONDS = "nanoseconds";

    private static final int AGGREGATE_ID_TYPE_GENERIC_PARAM_INDEX = 0;

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection"})
    private static class SqlDrafts {

        static final String INSERT_RECORD =
                "INSERT INTO %s " +
                " (" + ID + ", " + AGGREGATE + ", " + SECONDS + ", " + NANOSECONDS + ") " +
                " VALUES (?, ?, ?, ?);";

        static final String SELECT_BY_ID_SORTED_BY_TIME_DESC =
                "SELECT " + AGGREGATE + " FROM %s " +
                " WHERE " + ID + " = ? " +
                " ORDER BY " + SECONDS + " DESC, " + NANOSECONDS + " DESC;";

        static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID + " %s, " +
                    AGGREGATE + " BLOB, " +
                    SECONDS + " BIGINT, " +
                    NANOSECONDS + " INT " +
                ");";
    }

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = new HashSet<>();

    /**
     * Statements which are not closed yet.
     */
    private final Collection<PreparedStatement> statements = new HashSet<>();

    private final IdHelper<I> idHelper;

    private final String insertSql;
    private final String selectByIdSortedByTimeDescSql;

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
    static <I> JdbcAggregateStorage<I> newInstance(DataSourceWrapper dataSource, Class<? extends Aggregate<I, ?>> aggregateClass) {
        return new JdbcAggregateStorage<>(dataSource, aggregateClass);
    }

    private JdbcAggregateStorage(DataSourceWrapper dataSource, Class<? extends Aggregate<I, ?>> aggregateClass) {
        this.dataSource = dataSource;

        final String tableName = getTableName(aggregateClass);
        this.insertSql = String.format(SqlDrafts.INSERT_RECORD, tableName);
        this.selectByIdSortedByTimeDescSql = String.format(SqlDrafts.SELECT_BY_ID_SORTED_BY_TIME_DESC, tableName);

        this.idHelper = createHelper(aggregateClass);
        createTableIfDoesNotExist(tableName);
    }

    private static String getTableName(Class aggregateClass) {
        final String className = aggregateClass.getName();
        final String tableNameTmp = PATTERN_DOT.matcher(className).replaceAll(UNDERSCORE);
        return PATTERN_DOLLAR.matcher(tableNameTmp).replaceAll(UNDERSCORE).toLowerCase();
    }

    @SuppressWarnings("IfMayBeConditional")
    private IdHelper<I> createHelper(Class<? extends Aggregate> aggregateClass) {
        final IdHelper<I> helper;
        // TODO:2016-02-02:alexander.litus: find out why cannot use getClass() instead of aggregateClass here
        final Class<I> idClass = Classes.getGenericParameterType(aggregateClass, AGGREGATE_ID_TYPE_GENERIC_PARAM_INDEX);
        if (Long.class.isAssignableFrom(idClass)) {
            helper = new IdHelper.LongIdHelper<>();
        } else if (Integer.class.isAssignableFrom(idClass)) {
            helper = new IdHelper.IntIdHelper<>();
        } else {
            helper = new IdHelper.StringOrMessageIdHelper<>();
        }
        return helper;
    }

    private void createTableIfDoesNotExist(String tableName) throws DatabaseException {
        final String idColumnType = idHelper.getIdColumnType();
        final String createTableSql = String.format(SqlDrafts.CREATE_TABLE_IF_DOES_NOT_EXIST, tableName, idColumnType);
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
    protected void writeInternal(I id, AggregateStorageRecord record) throws DatabaseException {
        final byte[] serializedRecord = serialize(record);
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            insert(connection, id, serializedRecord, record);
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
        checkNotNull(id);
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final PreparedStatement statement = selectByIdStatement(connection, id);
            statements.add(statement);
            final DbIterator iterator = new DbIterator(statement);
            iterators.add(iterator);
            return iterator;
        }
    }

    private static class DbIterator implements Iterator<AggregateStorageRecord>, AutoCloseable {

        private final ResultSet resultSet;

        private boolean isHasNextCalledBeforeNext = false;

        private DbIterator(PreparedStatement selectByIdStatement) throws DatabaseException {
            try {
                this.resultSet = selectByIdStatement.executeQuery();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
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
        @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
        public AggregateStorageRecord next() {
            if (!isHasNextCalledBeforeNext) {
                throw new IllegalStateException("It is required to call hasNext() before next() method.");
            }
            isHasNextCalledBeforeNext = false;

            final byte[] bytes = readRecordBytes();
            final AggregateStorageRecord record = toRecord(bytes);
            return record;
        }

        private byte[] readRecordBytes() {
            try {
                final byte[] bytes = resultSet.getBytes(AGGREGATE);
                return bytes;
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

    private void insert(ConnectionWrapper connection,
                        I id,
                        byte[] serializedAggregate,
                        AggregateStorageRecord record) {
        try (PreparedStatement statement = insertRecordStatement(connection, id, serializedAggregate, record)) {
            statement.execute();
            connection.commit();
        } catch (SQLException e) {
            logTransactionError(id, e);
            connection.rollback();
            throw new DatabaseException(e);
        }
    }

    private PreparedStatement insertRecordStatement(ConnectionWrapper connection,
                                                    I id,
                                                    byte[] serializedRecord,
                                                    AggregateStorageRecord record) {
        final PreparedStatement statement = connection.prepareStatement(insertSql);
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

    private PreparedStatement selectByIdStatement(ConnectionWrapper connection, I id){
        final PreparedStatement statement = connection.prepareStatement(selectByIdSortedByTimeDescSql);
        idHelper.setId(1, id, statement);
        return statement;
    }

    private void logTransactionError(I id, Exception e) {
        log().error("Error during transaction, aggregate ID = " + id, e);
    }

    @Override
    public void close() throws DatabaseException {
        closeAll(iterators);
        iterators.clear();

        closeAll(statements);
        statements.clear();

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
