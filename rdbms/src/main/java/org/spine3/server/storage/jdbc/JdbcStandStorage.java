/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.StandStorage;
import org.spine3.server.storage.jdbc.entity.JdbcRecordStorage;
import org.spine3.server.storage.jdbc.entity.query.EntityStorageQueryFactory;
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
@SuppressWarnings("unchecked") // Due to lots of conversions (e.g. AggregateStateId <-> Object) IDE can't track types of collections.
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

    protected JdbcStandStorage(Builder builder) {
        super(builder.isMultitenant);
        recordStorage = JdbcRecordStorage.newInstance(
                checkNotNull(builder.dataSource),
                builder.isMultitenant,
                checkNotNull(builder.entityStorageQueryFactory),
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

    @Nullable
    @Override
    protected EntityStorageRecord readInternal(AggregateStateId id) {
        return recordStorage.read(id.getAggregateId());
    }

    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<AggregateStateId> ids) {
        final Collection pureIds = Collections2.transform(Lists.newArrayList(ids), ID_MAPPER);
        return recordStorage.readBulk(pureIds);
    }

    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<AggregateStateId> ids, FieldMask fieldMask) {
        final Collection pureIds = Collections2.transform(Lists.newArrayList(ids), ID_MAPPER);
        return recordStorage.readBulk(pureIds, fieldMask);
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllInternal() {
        return retrieveRecordsWithValidIds(recordStorage.readAll());
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllInternal(FieldMask fieldMask) {
        return retrieveRecordsWithValidIds(recordStorage.readAll(fieldMask));
    }

    @Override
    protected void writeInternal(AggregateStateId id, EntityStorageRecord record) {
        recordStorage.write(id.getAggregateId(), record);
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
     * @param <I> Id type of the {@link org.spine3.server.entity.Entity} that will the {@code JdbcStandStorage}
     *           created with this builder be used for.
     * @return New parametrized instance of {@link Builder}.
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * Builds instances of {@code JdbcStandStorage}.
     *
     * @param <I> Id type of the {@link org.spine3.server.entity.Entity} that will the {@code JdbcStandStorage} be used for.
     */
    public static class Builder<I> {

        private boolean isMultitenant;
        private DataSourceWrapper dataSource;
        private Descriptors.Descriptor stateDescriptor;
        private EntityStorageQueryFactory<I> entityStorageQueryFactory;

        private Builder() {
        }

        /**
         * Sets optional field {@code isMultitenant}. {@code false} is the default value.
         */
        public Builder setMultitenant(boolean multitenant) {
            isMultitenant = multitenant;
            return this;
        }

        /**
         * Sets required field {@code dataSource}.
         */
        public Builder setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets required field {@code stateDescriptor}.
         *
         * @param stateDescriptor {@link com.google.protobuf.Descriptors.Descriptor} of the {@link org.spine3.server.entity.Entity} state.
         */
        public Builder setStateDescriptor(Descriptors.Descriptor stateDescriptor) {
            this.stateDescriptor = stateDescriptor;
            return this;
        }

        /**
         * Sets required field {@code queryFactory}.
         */
        public Builder setEntityStorageQueryFactory(EntityStorageQueryFactory<I> queryFactory) {
            this.entityStorageQueryFactory = queryFactory;
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
