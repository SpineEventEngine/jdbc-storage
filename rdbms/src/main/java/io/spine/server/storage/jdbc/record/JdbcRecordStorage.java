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

package io.spine.server.storage.jdbc.record;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import io.spine.base.Identifier;
import io.spine.client.EntityFilters;
import io.spine.client.EntityId;
import io.spine.client.EntityIdFilter;
import io.spine.client.OrderBy;
import io.spine.client.Pagination;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.query.PairedValue;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

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

    /**
     * Creates a new instance using the builder.
     *
     * @param builder the storage builder
     */
    protected JdbcRecordStorage(Builder<I> builder) throws DatabaseException {
        super(builder.isMultitenant(), builder.getEntityClass());
        this.dataSource = builder.getDataSource();
        Class<? extends Entity<I, ?>> entityClass = builder.getEntityClass();
        Collection<EntityColumn> entityColumns = entityColumnCache().getColumns();
        this.table = new RecordTable<>(entityClass, dataSource, builder.getColumnTypeRegistry(),
                                       builder.getTypeMapping(), entityColumns);
        table.create();
    }

    @Override
    public Iterator<I> index() {
        return table.index();
    }

    @Override
    public boolean delete(I id) {
        checkNotNull(id);

        boolean result = table.delete(id);
        return result;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Optional<EntityRecord> readRecord(I id) throws DatabaseException {
        EntityRecord record = table.read(id);
        return Optional.ofNullable(record);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return readMultipleRecords(ids, FieldMask.getDefaultInstance());
    }

    @Override
    protected Iterator<@Nullable EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                                   FieldMask fieldMask) {
        EntityQuery<I> query = toQuery(ids);
        Iterator<EntityRecord> records = table.readByIds(query, fieldMask);
        return records;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        return readAllRecords(FieldMask.getDefaultInstance());
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(FieldMask fieldMask) {
        Iterator<EntityRecord> records = table.readByQuery(emptyQuery(), fieldMask);
        return records;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(EntityQuery<I> query, FieldMask fieldMask) {
        EntityQuery<I> completeQuery = appendLifecycleFilters(query);
        Iterator<EntityRecord> records = table.readByQuery(completeQuery, fieldMask);
        return records;
    }

    @Override
    protected void writeRecord(I id, EntityRecordWithColumns record) {
        table.write(id, record);
    }

    @Override
    protected void writeRecords(Map<I, EntityRecordWithColumns> records) {
        table.write(records);
    }

    @Override
    public void close() throws DatabaseException {
        super.close();
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

    private EntityQuery<I> emptyQuery() {
        EntityQuery<I> query = EntityQueries.from(
                EntityFilters.getDefaultInstance(),
                OrderBy.getDefaultInstance(),
                Pagination.getDefaultInstance(),
                getThis());
        EntityQuery<I> result = appendLifecycleFilters(query);
        return result;
    }

    private EntityQuery<I> toQuery(Iterable<I> ids) {
        Iterable<EntityId> entityIds = stream(ids.spliterator(), false)
                .map(AggregateStateIdToEntityId.INSTANCE)
                .collect(toList());
        EntityIdFilter idFilter = EntityIdFilter.newBuilder()
                                                .addAllIds(entityIds)
                                                .build();
        EntityFilters entityFilters = EntityFilters.newBuilder()
                                                   .setIdFilter(idFilter)
                                                   .build();
        EntityQuery<I> query = EntityQueries.from(entityFilters,
                                                  OrderBy.getDefaultInstance(),
                                                  Pagination.getDefaultInstance(),
                                                  getThis());
        EntityQuery<I> result = appendLifecycleFilters(query);
        return result;
    }

    private EntityQuery<I> appendLifecycleFilters(EntityQuery<I> query) {
        if (isLifecycleSupported() && !query.isLifecycleAttributesSet()) {
            return query.withActiveLifecycle(this);
        }
        return query;
    }

    private RecordStorage<I> getThis() {
        return this;
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
     * The builder for {@link JdbcRecordStorage}.
     */
    public static class Builder<I>
            extends StorageBuilder<Builder<I>, JdbcRecordStorage<I>> {

        private Class<? extends Entity<I, ?>> entityClass;
        private ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                columnTypeRegistry = JdbcTypeRegistryFactory.defaultInstance();

        private Builder() {
            super();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        public Class<? extends Entity<I, ?>> getEntityClass() {
            return entityClass;
        }

        /**
         * @param entityClass the class of entities to be stored
         */
        public Builder<I> setEntityClass(Class<? extends Entity<I, ?>> entityClass) {
            this.entityClass = checkNotNull(entityClass);
            return this;
        }

        /**
         * @param columnTypeRegistry the registry of entity columns to be used
         */
        public Builder<I> setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                        columnTypeRegistry) {
            this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
            return this;
        }

        public ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
        getColumnTypeRegistry() {
            return columnTypeRegistry;
        }

        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkNotNull(entityClass, "Entity class must be set");
            checkNotNull(columnTypeRegistry, "Column type registry must not be null.");
        }

        @Override
        public JdbcRecordStorage<I> doBuild() {
            return new JdbcRecordStorage<>(this);
        }
    }

    private enum AggregateStateIdToEntityId implements Function<Object, EntityId> {
        INSTANCE;

        @Override
        public EntityId apply(@Nullable Object genericId) {
            checkNotNull(genericId);
            Any content = Identifier.pack(genericId);
            EntityId id = EntityId.newBuilder()
                                  .setId(content)
                                  .build();
            return id;
        }
    }

    @VisibleForTesting
    RecordTable<I> getTable() {
        return table;
    }
}
