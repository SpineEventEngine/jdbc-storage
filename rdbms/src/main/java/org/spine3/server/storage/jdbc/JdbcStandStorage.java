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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.FieldMask;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.StandStorage;
import org.spine3.server.storage.jdbc.entity.JdbcRecordStorage;
import org.spine3.server.storage.jdbc.entity.query.EntityStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dmytro Dashenkov
 */
public class JdbcStandStorage extends StandStorage {

    private final JdbcRecordStorage<AggregateStateId> recordStorage;

    protected JdbcStandStorage(Builder builder) {
        super(builder.isMultitenant);
        recordStorage = JdbcRecordStorage.newInstance(builder.dataSource, builder.isMultitenant, builder.entityStorageQueryFactory);
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
        return recordStorage.read(id);
    }

    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<AggregateStateId> ids) {
        return recordStorage.readBulk(ids);
    }

    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<AggregateStateId> ids, FieldMask fieldMask) {
        return recordStorage.readBulk(ids, fieldMask);
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllInternal() {
        return recordStorage.readAll();
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllInternal(FieldMask fieldMask) {
        return recordStorage.readAll(fieldMask);
    }

    @Override
    protected void writeInternal(AggregateStateId id, EntityStorageRecord record) {

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private Builder() {
        }

        private boolean isMultitenant;
        private DataSourceWrapper dataSource;
        private EntityStorageQueryFactory<AggregateStateId> entityStorageQueryFactory;

        public Builder setMultitenant(boolean multitenant) {
            isMultitenant = multitenant;
            return this;
        }

        public Builder setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder setEntityStorageQueryFactory(EntityStorageQueryFactory<AggregateStateId> queryFactory) {
            this.entityStorageQueryFactory = queryFactory;
            return this;
        }

        public JdbcStandStorage build() {
            return new JdbcStandStorage(this);
        }
    }
}
