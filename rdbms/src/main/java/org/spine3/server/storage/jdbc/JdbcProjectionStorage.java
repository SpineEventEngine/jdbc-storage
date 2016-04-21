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

import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.ProjectionStorage;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.WriteQuery;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.util.DbTableNameFactory.newTableName;
import static org.spine3.validate.Validate.isDefault;

/**
 * The implementation of the projection storage based on the RDBMS.
 *
 * @param <Id> a type of projection IDs
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcProjectionStorage<Id> extends ProjectionStorage<Id> {

    /**
     * A suffix of a table name where the last event time is stored.
     */
    private static final String LAST_EVENT_TIME_TABLE_NAME_SUFFIX = "_last_event_time";

    /**
     * Last event time seconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SECONDS_COL = "seconds";

    /**
     * Last event time nanoseconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String NANOS_COL = "nanoseconds";

    private final DataSourceWrapper dataSource;

    private final JdbcEntityStorage<Id> entityStorage;

    private final String tableName;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource a data source used by an {@code entityStorage}
     * @param projectionClass a class of projections to store
     * @param entityStorage an entity storage to use
     * @param <Id> a type of projection IDs
     * @return a new storage instance
     */
    /*package*/ static <Id> ProjectionStorage<Id> newInstance(DataSourceWrapper dataSource,
                                                            Class<? extends Entity<Id, ?>> projectionClass,
                                                            JdbcEntityStorage<Id> entityStorage) throws DatabaseException {
        return new JdbcProjectionStorage<>(dataSource, projectionClass, entityStorage);
    }

    private JdbcProjectionStorage(DataSourceWrapper dataSource,
                                  Class<? extends Entity<Id, ?>> projectionClass,
                                  JdbcEntityStorage<Id> entityStorage) throws DatabaseException {
        this.dataSource = dataSource;
        this.entityStorage = entityStorage;
        this.tableName = newTableName(projectionClass) + LAST_EVENT_TIME_TABLE_NAME_SUFFIX;

        new CreateTableIfDoesNotExistQuery().execute(tableName);
    }

    @Override
    public void writeLastHandledEventTime(Timestamp time) throws DatabaseException {
        if (containsLastEventTime()) {
            new UpdateTimestampQuery(time).execute();
        } else {
            new InsertTimestampQuery(time).execute();
        }
    }

    private boolean containsLastEventTime() throws DatabaseException {
        final Timestamp time = readLastHandledEventTime();
        final boolean containsEventTime = time != null;
        return containsEventTime;
    }

    @Override
    @Nullable
    public Timestamp readLastHandledEventTime() throws DatabaseException {
        final Timestamp timestamp = new SelectTimestampQuery().execute();
        return timestamp;
    }

    @Override
    public EntityStorage<Id> getEntityStorage() {
        return entityStorage;
    }

    @Override
    public void close() throws DatabaseException {
        // close only entityStorage because it must close dataSource itself
        entityStorage.close();
        try {
            super.close();
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private class CreateTableIfDoesNotExistQuery {

        private static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    SECONDS_COL + " BIGINT, " +
                    NANOS_COL + " INT " +
                ");";

        private void execute(String tableName) throws DatabaseException {
            final String createTableSql = format(CREATE_TABLE_IF_DOES_NOT_EXIST, tableName);
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(createTableSql)) {
                statement.execute();
            } catch (SQLException e) {
                log().error("Failed to create a table with the name: " + tableName, e);
                throw new DatabaseException(e);
            }
        }
    }

    private class WriteTimestampQuery extends WriteQuery {

        private final String query;
        private final Timestamp timestamp;

        private WriteTimestampQuery(String query, Timestamp timestamp) {
            super(dataSource);
            this.query = query;
            this.timestamp = timestamp;
        }

        @Override
        protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(query);
            final long seconds = timestamp.getSeconds();
            final int nanos = timestamp.getNanos();
            try {
                statement.setLong(1, seconds);
                statement.setInt(2, nanos);
                return statement;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        @Override
        protected void logError(SQLException exception) {
            log().error("Failed to write last event timestamp.", exception);
        }
    }

    private class InsertTimestampQuery extends WriteTimestampQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT_QUERY =
                "INSERT INTO %s " +
                " (" + SECONDS_COL + ", " + NANOS_COL + ')' +
                " VALUES (?, ?);";

        private InsertTimestampQuery(Timestamp timestamp) {
            super(format(INSERT_QUERY, tableName), timestamp);
        }
    }

    private class UpdateTimestampQuery extends WriteTimestampQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String UPDATE_QUERY =
                "UPDATE %s SET " +
                SECONDS_COL + " = ?, " +
                NANOS_COL + " = ?;";

        private UpdateTimestampQuery(Timestamp timestamp) {
            super(format(UPDATE_QUERY, tableName), timestamp);
        }
    }

    private class SelectTimestampQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_QUERY = "SELECT " + SECONDS_COL + ", " + NANOS_COL + " FROM %s ;";

        private final String selectQuery;

        private SelectTimestampQuery() {
            this.selectQuery = format(SELECT_QUERY, tableName);
        }

        @Nullable
        private Timestamp execute() throws DatabaseException {
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(selectQuery);
                 ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                final long seconds = resultSet.getLong(SECONDS_COL);
                final int nanos = resultSet.getInt(NANOS_COL);
                final Timestamp time = Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
                if (isDefault(time)) {
                    return null;
                }
                return time;
            } catch (SQLException e) {
                log().error("Failed to read last event time.", e);
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
        private final Logger value = LoggerFactory.getLogger(JdbcProjectionStorage.class);
    }
}
