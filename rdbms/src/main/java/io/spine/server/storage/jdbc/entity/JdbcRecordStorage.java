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

package io.spine.server.storage.jdbc.entity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.FieldMask;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.builder.StorageBuilder;
import io.spine.server.storage.jdbc.table.entity.RecordTable;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;

import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @param <I> the type of entity IDs
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
public class JdbcRecordStorage<I> extends RecordStorage<I> {

    private final DataSourceWrapper dataSource;

    private final RecordTable<I> table;

    protected JdbcRecordStorage(DataSourceWrapper dataSource, boolean multitenant,
                                Class<Entity<I, ?>> entityClass)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.table = new RecordTable<>(entityClass, dataSource);
        table.createIfNotExists();
    }

    private JdbcRecordStorage(Builder<I> builder) {
        this(builder.getDataSource(), builder.isMultitenant(), builder.getEntityClass());
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords(EntityQuery<I> query, FieldMask fieldMask) {
        return null;
    }

    @Override
    protected void writeRecord(I id, EntityRecordWithColumns record) {

    }

    @Override
    public Iterator<I> index() {
        return null;
    }

    @Override
    public boolean delete(I id) {
        checkNotNull(id);

        final boolean result = table.delete(id);
        return result;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Optional<EntityRecord> readRecord(I id) throws DatabaseException {
        final EntityRecord record = table.read(id);
        return Optional.fromNullable(record);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return readMultipleRecords(ids, FieldMask.getDefaultInstance());
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                         FieldMask fieldMask) {
        final Map<?, EntityRecord> recordMap = table.read(ids, fieldMask);
        return ImmutableList.copyOf(recordMap.values());
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords() {
        return readAllRecords(FieldMask.getDefaultInstance());
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords(FieldMask fieldMask) {
        final Map<I, EntityRecord> records = table.readAll(fieldMask);
        return ImmutableMap.copyOf(records);
    }

//    /**
//     * {@inheritDoc}
//     *
//     * @throws DatabaseException if an error occurs during an interaction with the DB
//     */
//    @VisibleForTesting
//    @Override
//    protected void writeRecord(I id, EntityRecord record) throws DatabaseException {
//        checkArgument(record.hasState(), "entity state");
//
//        table.write(id, record);
//    }

    @Override
    protected void writeRecords(Map<I, EntityRecordWithColumns> records) {
//        table.write(records);
    }

    @Override
    public void close() throws DatabaseException {
        try {
            super.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        dataSource.close();
    }

    /**
     * Clears all data in the storage.
     *
     * <p>Used for testing purposes.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    void clear() throws DatabaseException {
        table.deleteAll();
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I>
            extends StorageBuilder<Builder<I>, JdbcRecordStorage<I>> {

        private Class<? extends Entity<I, ?>> entityClass;

        private Builder() {
            super();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        @SuppressWarnings("unchecked") // cast Class object to an object of its superclass Class
        public Class<Entity<I, ?>> getEntityClass() {
            return (Class<Entity<I, ?>>) entityClass;
        }

        public Builder<I> setEntityClass(Class<? extends Entity<I, ?>> entityClass) {
            this.entityClass = checkNotNull(entityClass);
            return this;
        }

        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkNotNull(entityClass, "Entity class must be set");
        }

        @Override
        public JdbcRecordStorage<I> doBuild() {
            return new JdbcRecordStorage<>(this);
        }
    }
}
