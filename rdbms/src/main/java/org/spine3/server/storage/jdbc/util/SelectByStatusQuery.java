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
import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;

/**
 * A query which obtains a {@link Message} by an ID.
 *
 * @param <S> a type of storage message IDs
 * @param <M> a type of messages to read
 * @author Alexander Litus
 */
@Internal
public class SelectByStatusQuery<S, M extends Message> {

    private final String query;
    private final DataSourceWrapper dataSource;
    private final IdColumn<S> idColumn;

    private String messageColumnName;
    private Descriptor messageDescriptor;

    /**
     * Creates a new query instance.
     *
     * @param query SQL select query which selects a message by an ID (must have one ID parameter to set)
     * @param dataSource a data source to use to obtain DB connections
     * @param idColumn a helper object used to set IDs to statements as parameters
     */
    protected SelectByStatusQuery(String query, DataSourceWrapper dataSource, IdColumn<S> idColumn) {
        this.query = query;
        this.dataSource = dataSource;
        this.idColumn = idColumn;
    }

    /**
     * Executes a query, obtains a serialized message and deserializes it.
     *
     * @param id a message ID
     * @return a message or {@code null} if there is no needed data
     * @throws DatabaseException if an error occurs during an interaction with the DB
     * @see Serializer#deserialize(byte[], Descriptor)
     */
    @Nullable
    public M execute(S id) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = prepareStatement(connection, id);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final M message = readMessage(resultSet);
            return message;
        } catch (SQLException e) {
            log().error("Error during reading a message, ID = " + idToString(id), e);
            throw new DatabaseException(e);
        }
    }

    /**
     * Retrieves a message from a DB result set.
     *
     * <p>The default implementation reads a message as byte array and deserializes it.
     * In order to do so, it is required to {@link #setMessageColumnName(String)} and
     * {@link #setMessageDescriptor(Descriptor)}.
     *
     * @param resultSet a data set with the cursor pointed to the first row
     * @return a message instance or {@code null} if the row does not contain the needed data
     * @throws SQLException if an error occurs during an interaction with the DB
     */
    @Nullable
    protected M readMessage(ResultSet resultSet) throws SQLException {
        checkNotNull(messageColumnName, "messageColumnName must be set.");
        checkNotNull(messageDescriptor, "messageDescriptor must be set.");
        final byte[] bytes = resultSet.getBytes(messageColumnName);
        if (bytes == null) {
            return null;
        }
        final M message = deserialize(bytes, messageDescriptor);
        return message;
    }

    /**
     * Sets a DB column name which contains serialized messages.
     * It is required in order to use the default {@link #readMessage(ResultSet)} implementation.
     */
    public void setMessageColumnName(String messageColumnName) {
        this.messageColumnName = messageColumnName;
    }

    /**
     * Sets a descriptor of the messages to read.
     * It is required in order to use the default {@link #readMessage(ResultSet)} implementation.
     */
    public void setMessageDescriptor(Descriptor messageDescriptor) {
        this.messageDescriptor = messageDescriptor;
    }

    private PreparedStatement prepareStatement(ConnectionWrapper connection, S id) {
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
        private final Logger value = LoggerFactory.getLogger(SelectByStatusQuery.class);
    }
}
