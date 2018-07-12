/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DatabaseException;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator over a {@link ResultSet} of storage records.
 *
 * <p>This class internally uses {@link ResultSet}.
 * See how to {@linkplain #close() finish} the usage of the iterator properly.
 *
 * <p>Uses {@link Serializer} to deserialize records.
 *
 * <p><b>NOTE:</b> {@code remove} operation is not supported.
 *
 * @param <R> type of storage records
 * @author Alexander Litus
 */
@Internal
public abstract class DbIterator<R> implements Iterator<R>, Closeable {

    private final ResultSet resultSet;
    private final String columnName;
    private boolean hasNextCalled = false;
    private boolean nextCalled = true;
    private boolean memoizedHasNext = false;

    /**
     * Creates a new iterator instance.
     *
     * @param resultSet  the results of a DB query to iterate over
     * @param columnName a name of a serialized storage record column
     */
    protected DbIterator(ResultSet resultSet, String columnName) {
        this.resultSet = resultSet;
        this.columnName = columnName;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Calls {@link #close()}, if {@link ResultSet#next()} returns {@code false}.
     *
     * @return {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (!nextCalled) {
            return memoizedHasNext;
        }
        try {
            boolean hasNextElem = resultSet.next();
            if (!hasNextElem) {
                close();
            }

            // ResultSet.previous() is not used here because some JDBC drivers do not support it.
            memoizedHasNext = hasNextElem;

            hasNextCalled = true;
            nextCalled = false;

            return hasNextElem;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public R next() {
        if (!memoizedHasNext && (hasNextCalled || !hasNext())) {
            throw noSuchElement();
        }

        hasNextCalled = false;
        memoizedHasNext = false;
        nextCalled = true;

        R result;
        try {
            result = readResult();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return result;
    }

    protected abstract R readResult() throws SQLException;

    @VisibleForTesting
    public ResultSet getResultSet() {
        return resultSet;
    }

    protected String getColumnName() {
        return columnName;
    }

    /**
     * Removal is unsupported.
     *
     * @throws UnsupportedOperationException always
     * @deprecated as unsupported
     */
    @Override
    @Deprecated
    public final void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Removing is not supported.");
    }

    /**
     * Closes {@link #resultSet} and the related {@link Statement} and {@link Connection}.
     *
     * <p>This method should be called either manually or called by {@link #hasNext()}.
     *
     * @throws DatabaseException if {@code SQLException} is occurred
     */
    @Override
    public void close() throws DatabaseException {
        try {
            if (!resultSet.isClosed()) {

                // Get statement before closing the result set, because PostgreSQL doesn't allow
                // to retrieve a statement if a result set is closed.
                // The same strategy to obtain the connection is also safer.
                Statement statement = resultSet.getStatement();
                resultSet.close();
                boolean statementClosed = statement == null || statement.isClosed();
                if (!statementClosed) {
                    Connection connection = statement.getConnection();
                    statement.close();
                    boolean connectionClosed = connection == null || connection.isClosed();
                    if (!connectionClosed) {
                        connection.close();
                    }
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static NoSuchElementException noSuchElement() {
        throw new NoSuchElementException("No elements remained.");
    }
}
