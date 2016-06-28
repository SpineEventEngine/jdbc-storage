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
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;

/**
 * An iterator over a {@link ResultSet} of storage records.
 *
 * <p>Uses {@link Serializer} to deserialize records.
 *
 * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before {@link Iterator#next()}.
 *
 * <p><b>NOTE:</b> {@code remove} operation is not supported.
 *
 * @param <Record> type of storage records
 * @author Alexander Litus
 */
@Internal
public class DbIterator<Record extends Message> implements Iterator<Record>, AutoCloseable {

    private final ResultSet resultSet;
    private final PreparedStatement statement;
    private final String columnName;
    private final Descriptor recordDescriptor;
    private boolean isHasNextCalledBeforeNext = false;
    private boolean hasNext = false;

    /**
     * Creates a new iterator instance.
     *
     * @param statement a statement used to retrieve a result set
     *                  (both statement and result set are closed in {@link #close()}).
     * @param columnName a name of a serialized storage record column
     * @param recordDescriptor a descriptor of a storage record
     * @throws DatabaseException if an error occurs during interaction with the DB
     */
    public DbIterator(PreparedStatement statement, String columnName, Descriptor recordDescriptor) throws DatabaseException {
        try {
            this.resultSet = statement.executeQuery();
            this.statement = statement;
            this.columnName = columnName;
            this.recordDescriptor = recordDescriptor;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public DbIterator(ResultSet resultSet, String columnName, Descriptor recordDescriptor) throws DatabaseException {
        this.resultSet = resultSet;
        this.statement = null;
        this.columnName = columnName;
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public boolean hasNext() {
        try {
            final boolean hasNextElem = resultSet.next();
            /** {@link ResultSet#previous()} is not used here because some JDBC drivers do not support it. */
            hasNext = hasNextElem;
            isHasNextCalledBeforeNext = true;
            return hasNextElem;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Record next() {
        if (!isHasNextCalledBeforeNext) {
            throw new IllegalStateException("It is required to call hasNext() before next() method.");
        }
        isHasNextCalledBeforeNext = false;
        if (!hasNext) {
            throw new NoSuchElementException("No elements remained.");
        }
        final byte[] bytes = readBytes();
        final Record record = deserialize(bytes, recordDescriptor);
        return record;
    }

    private byte[] readBytes() {
        try {
            final byte[] bytes = resultSet.getBytes(columnName);
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
            statement.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
