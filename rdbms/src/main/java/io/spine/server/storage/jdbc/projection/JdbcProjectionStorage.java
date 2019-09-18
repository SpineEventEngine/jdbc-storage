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

package io.spine.server.storage.jdbc.projection;

import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.spine.client.ResponseFormat;
import io.spine.server.entity.EntityRecord;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * The implementation of the projection storage based on the RDBMS.
 *
 * @param <I>
 *         a type of projection IDs
 * @see JdbcStorageFactory
 */
public class JdbcProjectionStorage<I> extends ProjectionStorage<I> {

    private final JdbcRecordStorage<I> recordStorage;

    private final LastHandledEventTimeTable table;

    private final String projectionId;

    /**
     * Creates a new instance using the builder.
     *
     * @param builder
     *         the storage builder
     */
    protected JdbcProjectionStorage(Builder<I> builder) throws DatabaseException {
        super(builder.isMultitenant());
        this.recordStorage = builder.getRecordStorage();
        this.projectionId = builder.getProjectionClass()
                                   .getName();
        this.table = new LastHandledEventTimeTable(builder.dataSource(),
                                                   builder.typeMapping());
        table.create();
    }

    @Override
    public Iterator<I> index() {
        return recordStorage.index();
    }

    @Override
    public void writeLastHandledEventTime(Timestamp time) throws DatabaseException {
        table.write(projectionId, time);
    }

    @Override
    public @Nullable Timestamp readLastHandledEventTime() throws DatabaseException {
        Timestamp timestamp = table.read(projectionId);
        return timestamp;
    }

    @Override
    public RecordStorage<I> recordStorage() {
        return recordStorage;
    }

    @Override
    public void close() throws DatabaseException {
        super.close();

        // close only recordStorage because it must close dataSource by itself
        recordStorage.close();
    }

    @Override
    public boolean delete(I id) {
        return recordStorage.delete(id);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                         FieldMask fieldMask) {
        return recordStorage.readMultiple(ids, fieldMask);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(ResponseFormat format) {
        return recordStorage.readAll(format);
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
     * The builder for {@link JdbcProjectionStorage}.
     */
    public static class Builder<I> extends StorageBuilder<Builder<I>,
            JdbcProjectionStorage<I>> {

        private JdbcRecordStorage<I> recordStorage;
        private Class<? extends Projection<I, ?, ?>> projectionClass;

        private Builder() {
            super();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        public JdbcRecordStorage<I> getRecordStorage() {
            return recordStorage;
        }

        /**
         * Sets a {@link RecordStorage} to store projection instances.
         */
        public Builder<I> setRecordStorage(JdbcRecordStorage<I> recordStorage) {
            this.recordStorage = checkNotNull(recordStorage);
            return this;
        }

        /**
         * {@inheritDoc}
         *
         * <p>The {@code JdbcProjectionStorage.Builder} checks the {@code recordStorage} and
         * {@code queryFactory} fields to be set.
         */
        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkState(recordStorage != null, "Record Storage must not be null.");
            checkState(projectionClass != null, "Projection class must no be null.");
        }

        @Override
        public JdbcProjectionStorage<I> doBuild() {
            return new JdbcProjectionStorage<>(this);
        }

        /**
         * Sets the class of projections to be stored.
         */
        public Builder<I> setProjectionClass(Class<? extends Projection<I, ?, ?>> projectionClass) {
            this.projectionClass = checkNotNull(projectionClass);
            return this;
        }

        public Class<? extends Projection<I, ?, ?>> getProjectionClass() {
            return projectionClass;
        }
    }
}
