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

package io.spine.server.storage.jdbc.aggregate;

import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.DbIterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.server.storage.jdbc.aggregate.Closeables.closeAll;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * <p>This storage contains 3 tables by default:
 * <ul>
 *     <li>{@link AggregateEventRecordTable}
 *     <li>{@link LifecycleFlagsTable}
 *     <li>{@link EventCountTable}
 * </ul>
 *
 * @param <I> the type of aggregate IDs
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 */
public class JdbcAggregateStorage<I> extends AggregateStorage<I> {

    private final DataSourceWrapper dataSource;

    /**
     * The {@linkplain #historyBackward(AggregateReadRequest) history} iterators,
     * which are not {@linkplain DbIterator#close() closed} yet.
     *
     * <p>{@link DbIterator} will be closed automatically only
     * if all elements were {@linkplain DbIterator#hasNext() iterated}.
     *
     * <p>Because history iterators are used to go through a part of a history,
     * they should be closed by the storage.
     */
    private final Collection<DbIterator> iterators = newLinkedList();
    private final AggregateEventRecordTable<I> mainTable;
    private final LifecycleFlagsTable<I> lifecycleFlagsTable;
    private final EventCountTable<I> eventCountTable;

    /**
     * Creates a new instance using the builder.
     *
     * @param builder the storage builder
     */
    protected JdbcAggregateStorage(Builder<I> builder) {
        super(builder.isMultitenant());
        Class<? extends Aggregate<I, ?, ?>> aggregateClass = builder.getAggregateClass();
        TypeMapping mapping = builder.getTypeMapping();
        this.dataSource = builder.getDataSource();
        this.mainTable = new AggregateEventRecordTable<>(aggregateClass, dataSource, mapping);
        this.lifecycleFlagsTable = new LifecycleFlagsTable<>(aggregateClass, dataSource, mapping);
        this.eventCountTable = new EventCountTable<>(aggregateClass, dataSource, mapping);
        mainTable.create();
        lifecycleFlagsTable.create();
        eventCountTable.create();
    }

    @Override
    public Iterator<I> index() {
        return mainTable.index();
    }

    @Override
    public int readEventCountAfterLastSnapshot(I id) {
        checkNotClosed();
        Integer value = eventCountTable.read(id);
        int result = value == null
                     ? 0
                     : value;
        return result;
    }

    @Override
    public Optional<LifecycleFlags> readLifecycleFlags(I id) {
        return Optional.ofNullable(lifecycleFlagsTable.read(id));
    }

    @Override
    public void writeLifecycleFlags(I id, LifecycleFlags status) {
        lifecycleFlagsTable.write(id, status);
    }

    @Override
    public void writeEventCountAfterLastSnapshot(I id, int count) {
        checkNotClosed();
        eventCountTable.write(id, count);
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeRecord(I id, AggregateEventRecord record) throws DatabaseException {
        mainTable.insert(id, record);
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
    protected Iterator<AggregateEventRecord> historyBackward(AggregateReadRequest<I> request)
            throws DatabaseException {
        checkNotNull(request);

        I id = request.getRecordId();
        int fetchSize = request.getBatchSize();
        SelectEventRecordsById<I> query = mainTable.composeSelectQuery(id);
        DbIterator<AggregateEventRecord> historyIterator = query.execute(fetchSize);
        iterators.add(historyIterator);
        return historyIterator;
    }

    /**
     * Closes the storage.
     *
     * <p>Unclosed {@linkplain #iterators history iterators}
     * produced by this storage will be closed together with the storage.
     *
     * @throws DatabaseException if the underlying datasource cannot be closed
     */
    @Override
    public void close() throws DatabaseException {
        super.close();
        closeAll(iterators);
        dataSource.close();
        iterators.clear();
    }

    /**
     * Creates the builder for the storage.
     *
     * @return the builder instance
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * The builder for {@link JdbcAggregateStorage}.
     */
    public static class Builder<I> extends StorageBuilder<Builder<I>, JdbcAggregateStorage<I>> {

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

        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkNotNull(aggregateClass);
        }

        public Class<? extends Aggregate<I, ?, ?>> getAggregateClass() {
            return aggregateClass;
        }

        /**
         * @param aggregateClass the class of aggregates to be stored
         */
        public Builder<I> setAggregateClass(Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
            this.aggregateClass = checkNotNull(aggregateClass);
            return this;
        }
    }
}
