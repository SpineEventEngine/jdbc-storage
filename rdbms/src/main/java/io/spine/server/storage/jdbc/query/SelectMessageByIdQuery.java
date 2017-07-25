/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.Identifier;
import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.Serializer;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A query which obtains a {@link Message} by an ID.
 *
 * @param <I> a type of storage message IDs
 * @param <M> a type of messages to read
 * @author Alexander Litus
 */
@Internal
public class SelectMessageByIdQuery<I, M extends Message> extends SelectByIdQuery<I, M> {

    private final String messageColumnName;
    private final Descriptor messageDescriptor;

    protected SelectMessageByIdQuery(
            Builder<? extends Builder, ? extends SelectMessageByIdQuery, I, M> builder) {
        super(builder);
        this.messageColumnName = builder.messageColumnName;
        this.messageDescriptor = builder.messageDescriptor;
    }

    /**
     * Executes a query, obtains a serialized message and deserializes it.
     *
     * @return a message or {@code null} if there is no needed data
     * @throws DatabaseException if an error occurs during an interaction with the DB
     * @see Serializer#deserialize
     */
    @Nullable
    @Override
    public M execute() throws DatabaseException {
        try (ConnectionWrapper connection = getConnection(true);
             PreparedStatement statement = prepareStatement(connection, getId());
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final M message = readMessage(resultSet);
            return message;
        } catch (SQLException e) {
            getLogger().error("Error during reading a message, ID = "
                              + Identifier.toString(getId()), e);
            throw new DatabaseException(e);
        }
    }

    /**
     * Retrieves a message from a DB result set.
     *
     * <p>The default implementation reads a message as byte array and deserializes it.
     *
     * @param resultSet a data set with the cursor pointed to the first row
     * @return a message instance or {@code null} if the row does not contain the needed data
     * @throws SQLException if an error occurs during an interaction with the DB
     */
    @Nullable
    protected M readMessage(ResultSet resultSet) throws SQLException {
        checkNotNull(messageColumnName);
        checkNotNull(messageDescriptor);
        final byte[] bytes = resultSet.getBytes(messageColumnName);
        if (bytes == null) {
            return null;
        }
        final M message = Serializer.deserialize(bytes, messageDescriptor);
        return message;
    }

    protected PreparedStatement prepareStatement(ConnectionWrapper connection, I id) {
        final PreparedStatement statement = prepareStatement(connection);
        getIdColumn().setId(getIdIndexInQuery(), id, statement);
        return statement;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public abstract static class Builder<B extends Builder<B, Q, I, R>,
                                         Q extends SelectMessageByIdQuery<I, R>,
                                         I,
                                         R extends Message>
            extends SelectByIdQuery.Builder<I, B, Q> {

        private String messageColumnName;
        private Descriptor messageDescriptor;

        public B setMessageColumnName(String messageColumnName) {
            this.messageColumnName = messageColumnName;
            return getThis();
        }

        public B setMessageDescriptor(Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }
    }
}
