/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.record.Serializer;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.record.column.BytesColumn.bytesColumnName;
import static java.util.Objects.requireNonNull;

/**
 * A query which obtains a {@link Message} by an ID.
 *
 * @param <I>
 *         a type of storage record IDs
 * @param <R>
 *         a type of records to read
 */
public abstract class SelectMessageByIdQuery<I, R extends Message>
        extends ReadByIdQuery<I, R>
        implements SelectQuery<R> {

    private final Descriptor messageDescriptor;

    protected SelectMessageByIdQuery(
            Builder<I, R, ? extends Builder<I, R, ?, ?>,
                    ? extends SelectMessageByIdQuery<I, R>> builder) {
        super(builder);
        this.messageDescriptor = requireNonNull(builder.tableSpec()).recordDescriptor();
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
    public final @Nullable R execute() throws DatabaseException {
        try (var resultSet = query().getResults()) {
            if (!resultSet.next()) {
                return null;
            }
            var message = readMessage(resultSet);
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
    protected @Nullable R readMessage(ResultSet resultSet) throws SQLException {
        checkNotNull(messageDescriptor);
        var bytes = resultSet.getBytes(bytesColumnName());
        if (bytes == null) {
            return null;
        }

        @SuppressWarnings("unchecked") // It's up to user to provide correct binary data for unpack.
        var message = (R) Serializer.deserialize(bytes, messageDescriptor);
        return message;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName" /* For simplicity. */)
    protected abstract static class Builder<I, R extends Message,
                                            B extends Builder<I, R, B, Q>,
                                            Q extends SelectMessageByIdQuery<I, R>>
            extends ReadByIdQuery.Builder<I, R, B, Q> {
    }
}
