/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.DatabaseException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A query which obtains a {@link Message} by an ID.
 *
 * @param <I>
 *         a type of storage message IDs
 * @param <M>
 *         a type of messages to read
 */
public abstract class SelectMessageByIdQuery<I, M extends Message>
        extends IdAwareQuery<I>
        implements SelectQuery<M> {

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
     * @throws DatabaseException
     *         if an error occurs during an interaction with the DB
     * @see Serializer#deserialize
     */
    @Override
    public final @Nullable M execute() throws DatabaseException {
        try (ResultSet resultSet = query().getResults()) {
            if (!resultSet.next()) {
                return null;
            }
            M message = readMessage(resultSet);
            return message;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Obtains a query to get {@link AbstractSQLQuery#getResults() results}.
     *
     * @return a query, which is ready for execution
     */
    protected abstract AbstractSQLQuery<?, ?> query();

    /**
     * Retrieves a message from a DB result set.
     *
     * <p>The default implementation reads a message as byte array and deserializes it.
     *
     * @param resultSet
     *         a data set with the cursor pointed to the first row
     * @return a message instance or {@code null} if the row does not contain the needed data
     * @throws SQLException
     *         if an error occurs during an interaction with the DB
     */
    protected @Nullable M readMessage(ResultSet resultSet) throws SQLException {
        checkNotNull(messageColumnName);
        checkNotNull(messageDescriptor);
        byte[] bytes = resultSet.getBytes(messageColumnName);
        if (bytes == null) {
            return null;
        }

        @SuppressWarnings("unchecked") // It's up to user to provide correct binary data for unpack.
        M message = (M) Serializer.deserialize(bytes, messageDescriptor);
        return message;
    }

    protected abstract static class Builder<B extends Builder<B, Q, I, R>,
                                            Q extends SelectMessageByIdQuery<I, R>,
                                            I,
                                            R extends Message>
            extends IdAwareQuery.Builder<I, B, Q> {

        private String messageColumnName;
        private Descriptor messageDescriptor;

        @CanIgnoreReturnValue
        public B setMessageColumnName(String messageColumnName) {
            this.messageColumnName = messageColumnName;
            return getThis();
        }

        @CanIgnoreReturnValue
        public B setMessageDescriptor(Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }
    }
}
