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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.FieldMask;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.stand.AggregateStateId;
import io.spine.server.stand.StandStorage;
import io.spine.server.storage.jdbc.builder.StorageBuilder;
import io.spine.server.storage.jdbc.entity.JdbcRecordStorage;
import io.spine.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * JDBC-based implementation of {@link StandStorage}.
 *
 * @author Dmytro Dashenkov
 */
public class JdbcStandStorage extends StandStorage {

    private final JdbcRecordStorage<Object> recordStorage;

    private static final Function<AggregateStateId, Object> ID_MAPPER =
            new Function<AggregateStateId, Object>() {
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
        super(builder.isMultitenant());
        recordStorage = JdbcRecordStorage.newBuilder()
                                         .setDataSource(builder.getDataSource())
                                         .setMultitenant(builder.isMultitenant())
                                         .setEntityClass(StandEntity.class)
                                         .build();
    }

    @Override
    public Iterator<AggregateStateId> index() {
        return null;
    }

    @Override
    public Iterator<EntityRecord> readAllByType(TypeUrl type) {
        return readAllByType(type, FieldMask.getDefaultInstance());
    }

    @Override
    public Iterator<EntityRecord> readAllByType(final TypeUrl type, FieldMask fieldMask) {
        final Iterator<EntityRecord> allRecords = readAll(fieldMask);
        final ImmutableList<EntityRecord> filteredRecords = ImmutableList.of();

        while(allRecords.hasNext()) {
            if(allRecords.next().getState().getTypeUrl().equals(type.toString())) {
                filteredRecords.builder().add(allRecords.next());
            }
        }

        return filteredRecords.iterator();
    }

    @Override
    public boolean delete(AggregateStateId id) {
        final Object aggregateId = id.getAggregateId();
        return recordStorage.delete(aggregateId);
    }

    @Override
    protected Optional<EntityRecord> readRecord(AggregateStateId id) {
        return recordStorage.read(id.getAggregateId());
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids) {
        final Collection<Object> genericIds = Collections2.transform(Lists.newArrayList(ids),
                                                                     ID_MAPPER);
        return recordStorage.readMultiple(genericIds);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids,
                                                         FieldMask fieldMask) {
        final Collection<Object> genericIds = Collections2.transform(Lists.newArrayList(ids),
                                                                     ID_MAPPER);
        return recordStorage.readMultiple(genericIds, fieldMask);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        return recordStorage.readAll();
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(FieldMask fieldMask) {
        return recordStorage.readAll(fieldMask);
    }

    @Override
    protected void writeRecord(AggregateStateId id, EntityRecordWithColumns record) {
        recordStorage.write(id.getAggregateId(), record);
    }

    @Override
    protected void writeRecords(Map<AggregateStateId, EntityRecordWithColumns> records) {
        final Map<Object, EntityRecordWithColumns> genericIdRecords = new HashMap<>(records.size());
        for (Map.Entry<AggregateStateId, EntityRecordWithColumns> record : records.entrySet()) {
            final Object genericId = record.getKey()
                                           .getAggregateId();
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

    /**
     * Creates new instance of {@link Builder}.
     *
     * @param <I> ID type of the {@link io.spine.server.entity.Entity} that will be stored in
     *            the {@code JdbcStandStorage}.
     * @return New parametrized instance of {@link Builder}.
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

    public static class StandEntity extends AbstractEntity<Object, EntityRecord> {

        /**
         * Creates new instance with the passed ID.
         *
         * @param id
         */
        protected StandEntity(AggregateStateId id) {
            super(id);
        }
    }
}
