/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;

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
 * @param <R>
 *         the type of storage records
 */
@Internal
public abstract class DbIterator<R> implements Iterator<R>, Closeable {

    private final ResultSet resultSet;
    private boolean hasNextCalled = false;
    private boolean nextCalled = true;
    private boolean memoizedHasNext = false;

    private DbIterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    /**
     * Creates a {@code DbIterator} over the given {@code ResultSet}.
     *
     * @param resultSet
     *         the results of a DB query to iterate over
     * @param columnReader
     *         the column reader which extracts the required column values from the result set
     * @param <R>
     *         the type of storage records
     * @return a new instance of {@code DbIterator}
     */
    public static <R> DbIterator<R> over(ResultSet resultSet, ColumnReader<R> columnReader) {
        checkNotNull(resultSet);
        checkNotNull(columnReader);
        return new SingleColumnIterator<>(resultSet, columnReader);
    }

    /**
     * Creates a {@code DbIterator} for the simultaneous iteration over the entity records and
     * their IDs in the {@code ResultSet}.
     *
     * @param resultSet
     *         the results of a DB query to iterate over
     * @param idColumnReader
     *         the reader of the ID column
     * @param valueColumnReader
     *         the reader of the column storing entity records
     * @param <I>
     *         the type of the storage record IDs
     * @param <V>
     *         the type of the column value
     * @return a new instance of {@code DbIterator}
     */
    public static <I, V> DbIterator<DoubleColumnRecord<I, V>>
    over(ResultSet resultSet,
         ColumnReader<I> idColumnReader,
         ColumnReader<V> valueColumnReader) {
        checkNotNull(resultSet);
        checkNotNull(idColumnReader);
        checkNotNull(valueColumnReader);
        return new DoubleColumnIterator<>(resultSet, idColumnReader, valueColumnReader);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Calls {@link #close()}, if {@link ResultSet#next()} returns {@code false}.
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
    public ResultSet resultSet() {
        return resultSet;
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

    /**
     * A {@code DbIterator} that iterates over a single column in the given {@code ResultSet}.
     *
     * @param <R>
     *         the type of the storage records
     */
    private static class SingleColumnIterator<R> extends DbIterator<R> {

        private final ColumnReader<R> columnReader;

        /**
         * Creates a new instance of the {@code SingleColumnIterator}.
         *
         * @param resultSet
         *         the SQL query results to iterate over
         * @param columnReader
         *         the column reader which extracts column values from the {@code ResultSet}
         */
        private SingleColumnIterator(ResultSet resultSet, ColumnReader<R> columnReader) {
            super(resultSet);
            this.columnReader = columnReader;
        }

        @Override
        protected R readResult() throws SQLException {
            R result = columnReader.readValue(resultSet());
            return result;
        }
    }

    /**
     * A {@code DbIterator} that iterates over a pair of columns in the given {@code ResultSet}
     * simultaneously.
     *
     * @param <R1>
     *         the type of the storage records in the first column
     * @param <R2>
     *         the type of the storage records in the second column
     */
    private static class DoubleColumnIterator<R1, R2>
            extends DbIterator<DoubleColumnRecord<R1, R2>> {

        private final ColumnReader<R1> firstColumnReader;
        private final ColumnReader<R2> secondColumnReader;

        /**
         * Creates a new instance of the {@code DoubleColumnIterator}.
         *
         * @param resultSet
         *         the SQL query results to iterate over
         * @param firstColumnReader
         *         the reader for the values of the first column
         * @param secondColumnReader
         *         the reader for the values of the second column
         */
        private DoubleColumnIterator(ResultSet resultSet,
                                     ColumnReader<R1> firstColumnReader,
                                     ColumnReader<R2> secondColumnReader) {
            super(resultSet);
            this.firstColumnReader = firstColumnReader;
            this.secondColumnReader = secondColumnReader;
        }

        @Override
        protected DoubleColumnRecord<R1, R2> readResult() throws SQLException {
            R1 value1 = firstColumnReader.readValue(resultSet());
            R2 value2 = secondColumnReader.readValue(resultSet());
            DoubleColumnRecord<R1, R2> result = DoubleColumnRecord.of(value1, value2);
            return result;
        }
    }

    /**
     * A holder of a pair of column values for the simultaneous iteration over a {@link ResultSet}.
     *
     * @param <V1>
     *         the type of the first column value
     * @param <V2>
     *         the type of the second column value
     */
    public static final class DoubleColumnRecord<V1, V2> {

        private final V1 first;
        private final V2 second;

        private DoubleColumnRecord(V1 first, V2 second) {
            this.first = first;
            this.second = second;
        }

        /**
         * Creates a new {@code DoubleColumnRecord} instance.
         */
        public static <V1, V> DoubleColumnRecord<V1, V> of(V1 first, V second) {
            checkNotNull(first);
            checkNotNull(second);
            return new DoubleColumnRecord<>(first, second);
        }

        public V1 first() {
            return first;
        }

        public V2 second() {
            return second;
        }
    }
}
