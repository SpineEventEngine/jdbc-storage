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

package io.spine.server.storage.jdbc.projection;

import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.entity.JdbcRecordStorage;
import io.spine.server.storage.jdbc.table.LastHandledEventTimeTable;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.entity.EntityRecord;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.builder.StorageBuilder;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.jdbc.util.DbTableNameFactory.newTableName;

/**
 * The implementation of the projection storage based on the RDBMS.
 *
 * @param <I> a type of projection IDs
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
public class JdbcProjectionStorage<I> extends ProjectionStorage<I> {

    private final JdbcRecordStorage<I> recordStorage;

    private final LastHandledEventTimeTable table;

    private final Class<? extends Projection<I, ?, ?>> projectionClass;

    private final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;

    protected JdbcProjectionStorage(JdbcRecordStorage<I> recordStorage,
                                    boolean multitenant,
                                    Class<? extends Projection<I, ?, ?>> projectionClass,
                                    DataSourceWrapper dataSource,
                                    ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry)
            throws DatabaseException {
        super(multitenant);
        this.recordStorage = recordStorage;
        this.projectionClass = projectionClass;
        this.table = new LastHandledEventTimeTable(dataSource, columnTypeRegistry);
        this.columnTypeRegistry = columnTypeRegistry;
        table.createIfNotExists();
    }

    private JdbcProjectionStorage(Builder<I> builder) {
        this(builder.getRecordStorage(),
             builder.isMultitenant(),
             builder.getProjectionClass(),
             builder.getDataSource(),
             builder.getColumnTypeRegistry());
    }

    @Override
    public Iterator<I> index() {
        return null;
    }

    @Override
    public void writeLastHandledEventTime(Timestamp time) throws DatabaseException {
        // Use type as an ID, since the records are mapped to entity types 1:1
        final String id = newTableName(projectionClass);
        table.write(id, time);
    }

    @Override
    @Nullable
    public Timestamp readLastHandledEventTime() throws DatabaseException {
        final Timestamp timestamp = table.read(newTableName(projectionClass));
        return timestamp;
    }

    @Override
    public RecordStorage<I> recordStorage() {
        return recordStorage;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            super.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        // close only recordStorage because it must close dataSource by itself
        recordStorage.close();
    }

    @Override
    public boolean delete(I id) {
        return recordStorage.delete(id);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return recordStorage.readMultiple(ids);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                         FieldMask fieldMask) {
        return recordStorage.readMultiple(ids, fieldMask);
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords() {
        return recordStorage.readAll();
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords(FieldMask fieldMask) {
        return recordStorage.readAll(fieldMask);
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageBuilder<Builder<I>,
                                                          JdbcProjectionStorage<I>> {
        private JdbcRecordStorage<I> recordStorage;
        private Class<? extends Projection<I, ?, ?>> projectionClass;
        private ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;
        private Builder() {
            super();
        }

        public ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> getColumnTypeRegistry() {
            return columnTypeRegistry;
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        public JdbcRecordStorage<I> getRecordStorage() {
            return recordStorage;
        }

        public Builder<I> setRecordStorage(JdbcRecordStorage<I> recordStorage) {
            this.recordStorage = recordStorage;
            return this;
        }

        /**
         * {@inheritDoc}
         *
         * <p>The {@link JdbcProjectionStorage.Builder} checks the {@code recordStorage} and
         * {@code queryFactory} fields to be set.
         */
        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkState(getRecordStorage() != null, "Record Storage must not be null.");
        }

        @Override
        public JdbcProjectionStorage<I> doBuild() {
            return new JdbcProjectionStorage<>(this);
        }

        public Builder<I> setProjectionClass(Class<? extends Projection<I, ?, ?>> projectionClass) {
            this.projectionClass = projectionClass;
            return this;
        }

        public Class<? extends Projection<I, ?, ?>> getProjectionClass() {
            return projectionClass;
        }
    }
}
