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

package org.spine3.server.storage.jdbc.aggregate;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.aggregate.AggregateEventRecord;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.builder.StorageBuilder;
import org.spine3.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.visibility.VisibilityHandler;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static org.spine3.server.storage.jdbc.util.Closeables.closeAll;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * <p> This storage contains 2 tables by default, they are described
 * in {@link org.spine3.server.storage.jdbc.aggregate.query.Table}.
 *
 * @param <I> the type of aggregate IDs
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
public class JdbcAggregateStorage<I> extends AggregateStorage<I> {

    private final DataSourceWrapper dataSource;

    /** Iterators which are not closed yet. */
    private final Collection<DbIterator> iterators = newLinkedList();

    /** Creates queries for interaction with database. */
    private final AggregateStorageQueryFactory<I> queryFactory;

    private final VisibilityHandler<I> visibilityHandler;

    protected JdbcAggregateStorage(DataSourceWrapper dataSource,
                                   boolean multitenant,
                                   AggregateStorageQueryFactory<I> queryFactory)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.queryFactory = queryFactory;
        queryFactory.setLogger(LogSingleton.INSTANCE.value);
        queryFactory.newCreateMainTableQuery()
                    .execute();
        queryFactory.newCreateEventCountTableQuery()
                    .execute();
        this.visibilityHandler = new VisibilityHandler<>(queryFactory);
        visibilityHandler.initialize();
    }

    private JdbcAggregateStorage(Builder<I> builder) {
        this(builder.getDataSource(), builder.isMultitenant(), builder.getQueryFactory());
    }

    @Override
    public int readEventCountAfterLastSnapshot(I id) {
        checkNotClosed();
        final Integer count = queryFactory.newSelectEventCountByIdQuery(id)
                                          .execute();
        if (count == null) {
            return 0;
        }
        return count;
    }

    @Override
    protected Optional<Visibility> readVisibility(I id) {
        return visibilityHandler.readVisibility(id);
    }

    @Override
    protected void writeVisibility(I id, Visibility status) {
        visibilityHandler.writeVisibility(id, status);
    }

    @Override
    protected void markArchived(I id) {
        visibilityHandler.markArchived(id);
    }

    @Override
    protected void markDeleted(I id) {
        visibilityHandler.markDeleted(id);
    }

    @Override
    public void writeEventCountAfterLastSnapshot(I id, int count) {
        checkNotClosed();
        if (containsEventCount(id)) {
            queryFactory.newUpdateEventCountQuery(id, count)
                        .execute();
        } else {
            queryFactory.newInsertEventCountQuery(id, count)
                        .execute();
        }
    }

    private boolean containsEventCount(I id) {
        final Integer count = queryFactory.newSelectEventCountByIdQuery(id)
                                          .execute();
        final boolean contains = count != null;
        return contains;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeRecord(I id, AggregateEventRecord record) throws DatabaseException {
        queryFactory.newInsertRecordQuery(id, record)
                    .execute();
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before
     * {@link Iterator#next()}.
     *
     * @return a new {@link DbIterator} instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Iterator<AggregateEventRecord> historyBackward(I id) throws DatabaseException {
        checkNotNull(id);
        final Iterator<AggregateEventRecord> iterator =
                queryFactory.newSelectByIdSortedByTimeDescQuery(id)
                            .execute();
        iterators.add((DbIterator) iterator);
        return iterator;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            super.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        closeAll(iterators);
        iterators.clear();
        dataSource.close();
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageBuilder<Builder<I>,
                                                          JdbcAggregateStorage<I>,
                                                          AggregateStorageQueryFactory<I>> {
        private Builder() {
            super();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        @Override
        public JdbcAggregateStorage<I> doBuild() {
            return new JdbcAggregateStorage<>(this);
        }
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcAggregateStorage.class);
    }
}
