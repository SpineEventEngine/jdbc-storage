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
import com.google.protobuf.Int32Value;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateEventRecord;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import org.spine3.server.storage.jdbc.builder.StorageBuilder;
import org.spine3.server.storage.jdbc.table.entity.aggregate.AggregateEventRecordTable;
import org.spine3.server.storage.jdbc.table.entity.aggregate.EventCountTable;
import org.spine3.server.storage.jdbc.table.entity.aggregate.VisibilityTable;
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

    private final AggregateEventRecordTable<I> mainTable;

    private final VisibilityTable<I> visibilityTable;

    private final EventCountTable<I> eventCountTable;

    protected JdbcAggregateStorage(DataSourceWrapper dataSource,
                                   boolean multitenant,
                                   Class<? extends Aggregate<I, ?, ?>> aggregateClass)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.mainTable = new AggregateEventRecordTable<>(aggregateClass, dataSource);
        this.visibilityTable = new VisibilityTable<>(aggregateClass, dataSource);
        this.eventCountTable = new EventCountTable<>(aggregateClass, dataSource);
        mainTable.createIfNotExists();
        visibilityTable.createIfNotExists();
        eventCountTable.createIfNotExists();
    }

    private JdbcAggregateStorage(Builder<I> builder) {
        this(builder.getDataSource(), builder.isMultitenant(), builder.getAggregateClass());
    }

    @Override
    public int readEventCountAfterLastSnapshot(I id) {
        checkNotClosed();
        final Int32Value value = eventCountTable.read(id);
        final int result = value == null
                           ? 0
                           : value.getValue();
        return result;
    }

    @Override
    protected Optional<Visibility> readVisibility(I id) {
        return Optional.fromNullable(visibilityTable.read(id));
    }

    @Override
    protected void writeVisibility(I id, Visibility status) {
        visibilityTable.write(id, status);
    }

    @Override
    protected void markArchived(I id) {
        visibilityTable.markArchived(id);
    }

    @Override
    protected void markDeleted(I id) {
        visibilityTable.markDeleted(id);
    }

    @Override
    public void writeEventCountAfterLastSnapshot(I id, int count) {
        checkNotClosed();
        final Int32Value record = Int32Value.newBuilder()
                                            .setValue(count)
                                            .build();
        eventCountTable.write(id, record);
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeRecord(I id, AggregateEventRecord record) throws DatabaseException {
        checkNotClosed();
        checkNotNull(id);
        checkNotNull(record);
        mainTable.write(id, record);
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
        final DbIterator<AggregateEventRecord> result = mainTable.historyBackward(id);
        iterators.add(result);
        return result;
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
        private Class<? extends Aggregate<I, ?, ?>> aggregateClass;

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

        public Class<? extends Aggregate<I,?,?>> getAggregateClass() {
            return aggregateClass;
        }

        public Builder<I> setAggregateClass(Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
            this.aggregateClass = aggregateClass;
            return this;
        }
    }
}
