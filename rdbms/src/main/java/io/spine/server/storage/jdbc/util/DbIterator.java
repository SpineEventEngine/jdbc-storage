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

package io.spine.server.storage.jdbc.util;

import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DatabaseException;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator over a {@link ResultSet} of storage records.
 *
 * <p>Uses {@link Serializer} to deserialize records.
 *
 * <p><b>NOTE:</b> {@code remove} operation is not supported.
 *
 * @param <R> type of storage records
 * @author Alexander Litus
 */
@Internal
public abstract class DbIterator<R> implements Iterator<R>, AutoCloseable {

    private final ResultSet resultSet;
    @Nullable
    private final PreparedStatement statement;
    private final String columnName;
    private boolean isHasNextCalledBeforeNext = false;
    private boolean hasNext = false;

    /**
     * Creates a new iterator instance.
     *
     * @param statement        a statement used to retrieve a result set
     *                         (both statement and result set are closed in {@link #close()}).
     * @param columnName       a name of a serialized storage record column
     * @throws DatabaseException if an error occurs during interaction with the DB
     */
    protected DbIterator(PreparedStatement statement, String columnName) throws DatabaseException {
        try {
            this.resultSet = statement.executeQuery();
            this.columnName = columnName;
            this.statement = statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Creates a new iterator instance.
     *
     * @param resultSet  the results of a DB query to iterate over
     * @param columnName a name of a serialized storage record column
     * @throws DatabaseException if an error occurs during interaction with the DB
     */
    protected DbIterator(ResultSet resultSet, String columnName) {
        this.resultSet = resultSet;
        this.columnName = columnName;
        this.statement = null;
    }

    @Override
    public boolean hasNext() {
        try {
            final boolean hasNextElem = resultSet.next();
            // ResultSet.previous() is not used here because some JDBC drivers do not support it.
            hasNext = hasNextElem;
            isHasNextCalledBeforeNext = true;
            return hasNextElem;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public R next() {
        if (!hasNext && (isHasNextCalledBeforeNext || !hasNext())) {
            throw noSuchElement();
        }
        isHasNextCalledBeforeNext = false;
        hasNext = false;
        final R result;
        try {
            result = readResult();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return result;
    }

    protected abstract R readResult() throws SQLException;

    protected ResultSet getResultSet() {
        return resultSet;
    }

    protected String getColumnName() {
        return columnName;
    }

    /**
     * Removal is unsupported.
     *
     * @deprecated as unsupported
     * @throws UnsupportedOperationException always
     */
    @Override
    @Deprecated
    public final void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Removing is not supported.");
    }

    @Override
    public void close() throws DatabaseException {
        try {
            resultSet.close();
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static NoSuchElementException noSuchElement() {
        throw new NoSuchElementException("No elements remained.");
    }
}
