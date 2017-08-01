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

package io.spine.server.storage.jdbc;

import com.google.common.base.Converter;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import io.spine.client.EntityFilters;
import io.spine.client.EntityId;
import io.spine.client.EntityIdFilter;
import io.spine.server.entity.AbstractVersionableEntity;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.stand.AggregateStateId;
import io.spine.server.stand.StandStorage;
import io.spine.string.Stringifier;
import io.spine.string.StringifierRegistry;
import io.spine.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Lists.newArrayList;
import static io.spine.protobuf.TypeConverter.toAny;
import static io.spine.util.Exceptions.illegalStateWithCauseOf;
import static java.util.Collections.singleton;

/**
 * JDBC-based implementation of {@link StandStorage}.
 *
 * @author Dmytro Dashenkov
 */
public class JdbcStandStorage extends StandStorage {

    private static final EntityQuery<String> EMPTY_QUERY = EntityQueries.from(
            EntityFilters.getDefaultInstance(),
            Entity.class
    );

    private static final Converter<AggregateStateId, String> ID_MAPPER;

    private final JdbcRecordStorage<String> recordStorage;

    static {
        try {
            // Ensure the AggregateStateId Stringifier is registered.
            Class.forName(AggregateStateId.class.getCanonicalName());
        } catch (ClassNotFoundException e) {
            throw illegalStateWithCauseOf(e);
        }
        final Optional<Stringifier<AggregateStateId>> stringifier =
                StringifierRegistry.getInstance().get(AggregateStateId.class);
        checkState(stringifier.isPresent(), "AggregateStateId Stringifier is not registered!");
        ID_MAPPER = stringifier.get();
    }

    protected JdbcStandStorage(Builder builder) {
        super(builder.isMultitenant());
        recordStorage = JdbcRecordStorage.<String>newBuilder()
                                         .setDataSource(builder.getDataSource())
                                         .setMultitenant(builder.isMultitenant())
                                         .setEntityClass(StandEntity.class)
                                         .build();
    }

    @Override
    public Iterator<AggregateStateId> index() {
        final Iterator<String> index = recordStorage.index();
        final Iterator<AggregateStateId> result = Iterators.transform(index, ID_MAPPER.reverse());
        return result;
    }

    @Override
    public Iterator<EntityRecord> readAllByType(TypeUrl type) {
        return readAllByType(type, FieldMask.getDefaultInstance());
    }

    @Override
    public Iterator<EntityRecord> readAllByType(final TypeUrl type, FieldMask fieldMask) {
        final Iterator<EntityRecord> allRecords = readAll(fieldMask);
        final String requiredTypeUrl = type.value();
        final Iterator<EntityRecord> result = filter(allRecords, new Predicate<EntityRecord>() {
            // TODO:2017-07-14:dmytro.dashenkov: Replace in-memory filtering with SQL query.
            @Override
            public boolean apply(@Nullable EntityRecord entityRecord) {
                checkNotNull(entityRecord);
                final String typeUrl = entityRecord.getState().getTypeUrl();
                return typeUrl.equals(requiredTypeUrl);
            }
        });
        return result;
    }

    @Override
    public boolean delete(AggregateStateId id) {
        final String aggregateId = ID_MAPPER.convert(id);
        checkNotNull(aggregateId);
        return recordStorage.delete(aggregateId);
    }

    @Override
    protected Optional<EntityRecord> readRecord(AggregateStateId id) {
        final String aggregateId = ID_MAPPER.convert(id);
        checkNotNull(aggregateId);
        final Iterator<EntityRecord> read = readMultipleRecords(singleton(id));
        final List<EntityRecord> readList = newArrayList(read);
        checkState(readList.size() <= 1);
        if (readList.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(readList.get(0));
        }
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids) {
        final Iterable<String> genericIds = ID_MAPPER.convertAll(ids);
        return recordStorage.readMultiple(genericIds);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids,
                                                         FieldMask fieldMask) {
        final EntityQuery<String> query = idsToQuery(ids);
        return recordStorage.readAll(query, fieldMask);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        return recordStorage.readAll(EMPTY_QUERY, FieldMask.getDefaultInstance());
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(FieldMask fieldMask) {
        return recordStorage.readAll(EMPTY_QUERY, fieldMask);
    }

    @Override
    protected void writeRecord(AggregateStateId id, EntityRecordWithColumns record) {
        final String aggregateId = ID_MAPPER.convert(id);
        checkNotNull(aggregateId);
        recordStorage.write(aggregateId, record);
    }

    @Override
    protected void writeRecords(Map<AggregateStateId, EntityRecordWithColumns> records) {
        final Map<String, EntityRecordWithColumns> genericIdRecords = new HashMap<>(records.size());
        for (Map.Entry<AggregateStateId, EntityRecordWithColumns> record : records.entrySet()) {
            final String genericId = ID_MAPPER.convert(record.getKey());
            final EntityRecordWithColumns recordValue = record.getValue();
            genericIdRecords.put(genericId, recordValue);
        }
        recordStorage.write(genericIdRecords);
    }

    /**
     * Sets instance state to "closed" and closes used {@link JdbcRecordStorage}.
     */
    @Override
    public void close() {
        super.close();
        recordStorage.close();
    }

    private static EntityQuery<String> idsToQuery(Iterable<AggregateStateId> ids) {
        final Iterable<EntityId> entityIds = transform(ids, AggregateStateIdToEntityId.INSTANCE);
        final EntityIdFilter idFilter = EntityIdFilter.newBuilder()
                                                      .addAllIds(entityIds)
                                                      .build();
        final EntityFilters entityFilters = EntityFilters.newBuilder()
                                                         .setIdFilter(idFilter)
                                                         .build();
        final EntityQuery<String> query = EntityQueries.from(entityFilters, Entity.class);
        return query;
    }

    /**
     * Creates new instance of {@link Builder}.
     *
     * @param <I> ID type of the {@link io.spine.server.entity.Entity} that will be stored in
     *            the {@code JdbcStandStorage}.
     * @return new parametrized instance of {@link Builder}.
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * Builds instances of {@code JdbcStandStorage}.
     *
     * @param <I> ID type of the {@link io.spine.server.entity.Entity} that will be stored in
     *            the {@code JdbcStandStorage}.
     */
    public static class Builder<I> extends StorageBuilder<Builder<I>, JdbcStandStorage> {

        private Builder() {
            super();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        @Override
        public JdbcStandStorage doBuild() {
            return new JdbcStandStorage(this);
        }
    }

    public static class StandEntity extends AbstractVersionableEntity<String, EntityRecord> {

        /**
         * Creates new instance with the passed ID.
         */
        protected StandEntity(String id) {
            super(id);
        }
    }

    private enum AggregateStateIdToEntityId implements Function<AggregateStateId, EntityId> {
        INSTANCE;

        @Override
        public EntityId apply(@Nullable AggregateStateId aggregateStateId) {
            checkNotNull(aggregateStateId);
            final String stringId = ID_MAPPER.convert(aggregateStateId);
            checkNotNull(stringId);
            final Any content = toAny(stringId);
            final EntityId id = EntityId.newBuilder()
                                        .setId(content)
                                        .build();
            return id;
        }
    }
}
