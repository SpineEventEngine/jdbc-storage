/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
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
import io.spine.server.storage.jdbc.query.DbIterator.DoubleColumnRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * <p>This storage contains two tables by default:
 * <ol>
 *     <li>{@link AggregateEventRecordTable}
 *     <li>{@link LifecycleFlagsTable}
 * </ol>
 *
 * @param <I>
 *         the type of aggregate IDs
 */
public class JdbcAggregateStorage<I> extends AggregateStorage<I> {

    private final DataSourceWrapper dataSource;

    private final AggregateEventRecordTable<I> mainTable;
    private final LifecycleFlagsTable<I> lifecycleFlagsTable;

    /**
     * Creates a new instance using the builder.
     *
     * @param builder
     *         the storage builder
     */
    protected JdbcAggregateStorage(Builder<I> builder) {
        super(builder.isMultitenant());
        Class<? extends Aggregate<I, ?, ?>> aggregateClass = builder.getAggregateClass();
        TypeMapping mapping = builder.typeMapping();
        this.dataSource = builder.dataSource();
        this.mainTable = new AggregateEventRecordTable<>(aggregateClass, dataSource, mapping);
        this.lifecycleFlagsTable = new LifecycleFlagsTable<>(aggregateClass, dataSource, mapping);
        mainTable.create();
        lifecycleFlagsTable.create();
    }

    @Override
    public Optional<LifecycleFlags> readLifecycleFlags(I id) {
        return Optional.ofNullable(lifecycleFlagsTable.read(id));
    }

    @Override
    public void writeLifecycleFlags(I id, LifecycleFlags status) {
        lifecycleFlagsTable.write(id, status);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Any exceptions occurred in this operation are propagated as {@link DatabaseException}.
     *
     * @throws DatabaseException
     *         if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeRecord(I id, AggregateEventRecord record) throws DatabaseException {
        mainTable.insert(id, record);
    }

    @Override
    protected Iterator<AggregateEventRecord> historyBackward(AggregateReadRequest<I> request)
            throws DatabaseException {
        checkNotNull(request);

        I id = request.recordId();
        int fetchSize = request.batchSize();
        SelectEventRecordsById<I> query = mainTable.composeSelectQuery(id);
        DbIterator<AggregateEventRecord> historyIterator = query.execute(fetchSize);
        ImmutableList<AggregateEventRecord> records = ImmutableList.copyOf(historyIterator);
        historyIterator.close();
        return records.iterator();
    }

    @Override
    protected void truncate(int snapshotIndex) {
        doTruncate(snapshotIndex, null);
    }

    @Override
    protected void truncate(int snapshotIndex, Timestamp date) {
        doTruncate(snapshotIndex, date);
    }

    @Override
    protected Iterator<I> distinctAggregateIds() {
        return mainTable.index();
    }

    @SuppressWarnings("TryFinallyCanBeTryWithResources")    /* For better readability. */
    private void doTruncate(int snapshotIndex, @Nullable Timestamp date) {
        DbIterator<DoubleColumnRecord<I, Integer>> records =
                selectVersionsToPersist(snapshotIndex, date);
        try {
            stream(records)
                    .forEach(record -> mainTable.deletePriorRecords(record.first(),
                                                                    record.second()));
        } finally {
            records.close();
        }
    }

    private DbIterator<DoubleColumnRecord<I, Integer>>
    selectVersionsToPersist(int snapshotIndex, @Nullable Timestamp date) {
        SelectVersionBySnapshot<I> query =
                mainTable.composeSelectVersionQuery(snapshotIndex, date);
        DbIterator<DoubleColumnRecord<I, Integer>> iterator = query.execute();
        return iterator;
    }

    /**
     * Closes the storage.
     *
     * @throws DatabaseException
     *         if the underlying datasource cannot be closed
     */
    @Override
    public void close() throws DatabaseException {
        super.close();
        dataSource.close();
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
         * Sets the class of aggregates to be stored.
         */
        public Builder<I> setAggregateClass(Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
            this.aggregateClass = checkNotNull(aggregateClass);
            return this;
        }
    }
}
