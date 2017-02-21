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

package org.spine3.server.storage.jdbc;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.stand.StandStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.entity.JdbcRecordStorage;
import org.spine3.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JDBC-based implementation of {@link StandStorage}.
 *
 * @author Dmytro Dashenkov
 */
public class JdbcStandStorage extends StandStorage {

    private final JdbcRecordStorage<Object> recordStorage;

    private static final Function<AggregateStateId, Object> ID_MAPPER = new Function<AggregateStateId, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable AggregateStateId input) {
            if (input == null) {
                return null;
            }
            return input.getAggregateId();
        }
    };

    @SuppressWarnings("unchecked")
    protected JdbcStandStorage(Builder builder) {
        super(builder.isMultitenant);
        recordStorage = JdbcRecordStorage.newInstance(
                checkNotNull(builder.dataSource),
                builder.isMultitenant,
                checkNotNull(builder.recordStorageQueryFactory),
                checkNotNull(builder.stateDescriptor));
    }

    @Override
    public ImmutableCollection<EntityStorageRecord> readAllByType(TypeUrl type) {
        return readAllByType(type, FieldMask.getDefaultInstance());
    }

    @Override
    public ImmutableCollection<EntityStorageRecord> readAllByType(final TypeUrl type, FieldMask fieldMask) {
        final Map<AggregateStateId, EntityStorageRecord> allRecords = readAll(fieldMask);
        final Map<AggregateStateId, EntityStorageRecord> resultMap = Maps.filterKeys(allRecords, new Predicate<AggregateStateId>() {
            @Override
            public boolean apply(@Nullable AggregateStateId stateId) {
                checkNotNull(stateId);
                final boolean typeMatches = stateId.getStateType()
                        .equals(type);
                return typeMatches;
            }
        });

        final ImmutableList<EntityStorageRecord> result = ImmutableList.copyOf(resultMap.values());
        return result;
    }

    @Override
    public boolean markArchived(AggregateStateId id) {
        return false;
    }

    @Override
    public boolean markDeleted(AggregateStateId id) {
        return false;
    }

    @Override
    public boolean delete(AggregateStateId id) {
        return false;
    }

    @Nullable
    @Override
    protected Optional<EntityStorageRecord> readRecord(AggregateStateId id) {
        return recordStorage.read(id.getAggregateId());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<AggregateStateId> ids) {
        final Collection pureIds = Collections2.transform(Lists.newArrayList(ids), ID_MAPPER);
        return recordStorage.readMultiple(pureIds);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<AggregateStateId> ids, FieldMask fieldMask) {
        final Collection pureIds = Collections2.transform(Lists.newArrayList(ids), ID_MAPPER);
        return recordStorage.readMultiple(pureIds, fieldMask);
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllRecords() {
        return retrieveRecordsWithValidIds(recordStorage.readAll());
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllRecords(FieldMask fieldMask) {
        return retrieveRecordsWithValidIds(recordStorage.readAll(fieldMask));
    }

    @Override
    protected void writeRecord(AggregateStateId id, EntityStorageRecord record) {
        recordStorage.write(id.getAggregateId(), record);
    }

    @Override
    protected void writeRecords(Map<AggregateStateId, EntityStorageRecord> records) {

    }

    /**
     * Sets instance state to "closed" and closes used {@link JdbcRecordStorage}.
     */
    @Override
    public void close() throws Exception {
        super.close();
        recordStorage.close();
    }

    private static Map<AggregateStateId, EntityStorageRecord> retrieveRecordsWithValidIds(Map<?, EntityStorageRecord> records) {
        final ImmutableMap.Builder<AggregateStateId, EntityStorageRecord> result = new ImmutableMap.Builder<>();

        for (Map.Entry<?, EntityStorageRecord> entry : records.entrySet()) {
            final AggregateStateId id = AggregateStateId.of(entry.getKey(), TypeUrl.of(entry.getValue().getState().getTypeUrl()));
            result.put(id, entry.getValue());
        }

        return result.build();
    }

    /**
     * Creates new instance of {@link Builder}.
     *
     * @param <I> ID type of the {@link org.spine3.server.entity.Entity} that will be stored in the {@code JdbcStandStorage}.
     * @return New parametrized instance of {@link Builder}.
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * Builds instances of {@code JdbcStandStorage}.
     *
     * @param <I> ID type of the {@link org.spine3.server.entity.Entity} that will be stored in the {@code JdbcStandStorage}.
     */
    public static class Builder<I> {

        private boolean isMultitenant;
        private DataSourceWrapper dataSource;
        private Descriptors.Descriptor stateDescriptor;
        private RecordStorageQueryFactory<I> recordStorageQueryFactory;

        private Builder() {
        }

        /**
         * Sets optional field {@code isMultitenant}. {@code false} is the default value.
         */
        public Builder<I> setMultitenant(boolean multitenant) {
            isMultitenant = multitenant;
            return this;
        }

        /**
         * Sets required field {@code dataSource}.
         */
        public Builder<I> setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets required field {@code stateDescriptor}.
         *
         * @param stateDescriptor {@link com.google.protobuf.Descriptors.Descriptor} of the {@link org.spine3.server.entity.Entity} state.
         */
        public Builder<I> setStateDescriptor(Descriptors.Descriptor stateDescriptor) {
            this.stateDescriptor = stateDescriptor;
            return this;
        }

        /**
         * Sets required field {@code queryFactory}.
         */
        public Builder<I> setRecordStorageQueryFactory(RecordStorageQueryFactory<I> queryFactory) {
            this.recordStorageQueryFactory = queryFactory;
            return this;
        }

        /**
         * @return New instance of {@code JdbcStandStorage}.
         */
        public JdbcStandStorage build() {
            return new JdbcStandStorage(this);
        }
    }
}
