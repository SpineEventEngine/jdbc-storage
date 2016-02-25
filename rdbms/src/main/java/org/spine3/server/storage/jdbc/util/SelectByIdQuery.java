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

package org.spine3.server.storage.jdbc.util;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.jdbc.util.Serializer.deserializeMessage;

/**
 * A query which obtains a storage record by an ID.
 *
 * @param <I> a type of storage record IDs
 * @param <R> a type of storage records
 * @author Alexander Litus
 */
@Internal
public class SelectByIdQuery<I, R extends Message> {

    private final String query;
    private final DataSourceWrapper dataSource;
    private final IdColumn<I> idColumn;

    private String recordColumnName;
    private Descriptor recordDescriptor;

    /**
     * Creates a new query instance.
     *
     * @param query SQL select query which selects records by an ID (must have one ID parameter)
     * @param dataSource a data source to use to obtain DB connections
     * @param idColumn a helper object used to set IDs to statements as parameters
     */
    protected SelectByIdQuery(String query, DataSourceWrapper dataSource, IdColumn<I> idColumn) {
        this.query = query;
        this.dataSource = dataSource;
        this.idColumn = idColumn;
    }

    /**
     * Executes a query, obtains a serialized record and deserializes it.
     *
     * @param id a record ID
     * @return a storage record {@link Message} or {@code null} if there is no needed data
     * @throws DatabaseException if an error occurs during an interaction with the DB
     * @see Serializer#deserializeMessage(byte[], Descriptor)
     */
    @Nullable
    public R execute(I id) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = prepareStatement(connection, id);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final R record = readRecord(resultSet);
            return record;
        } catch (SQLException e) {
            log().error("Error during reading a record, ID = " + idToString(id), e);
            throw new DatabaseException(e);
        }
    }

    /**
     * Retrieves a record from a DB result set.
     *
     * <p>The default implementation reads a record as byte array and deserializes it.
     * In order to do so, it is required to {@link #setRecordColumnName(String)} and
     * {@link #setRecordDescriptor(Descriptor)}.
     *
     * @param resultSet a data set with the cursor pointed to the first row
     * @return a record instance or {@code null} if the row does not contain the needed data
     * @throws SQLException if an error occurs during an interaction with the DB
     */
    @Nullable
    protected R readRecord(ResultSet resultSet) throws SQLException {
        checkNotNull(recordColumnName, "Record column name must be set.");
        checkNotNull(recordDescriptor, "Record descriptor must be set.");
        final byte[] bytes = resultSet.getBytes(recordColumnName);
        if (bytes == null) {
            return null;
        }
        final R record = deserializeMessage(bytes, recordDescriptor);
        return record;
    }

    /**
     * Sets a DB column name which contains serialized records.
     * It is required in order to use the default {@link #readRecord(ResultSet)} implementation.
     */
    public void setRecordColumnName(String recordColumnName) {
        this.recordColumnName = recordColumnName;
    }

    /**
     * Sets a descriptor of the storage record message.
     * It is required in order to use the default {@link #readRecord(ResultSet)} implementation.
     */
    public void setRecordDescriptor(Descriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    private PreparedStatement prepareStatement(ConnectionWrapper connection, I id) {
        final PreparedStatement statement = connection.prepareStatement(query);
        idColumn.setId(1, id, statement);
        return statement;
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(SelectByIdQuery.class);
    }
}
