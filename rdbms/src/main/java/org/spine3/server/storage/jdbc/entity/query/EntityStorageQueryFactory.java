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

package org.spine3.server.storage.jdbc.entity.query;

import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

/**
 * This class creates queries for interaction with {@link EntityTable}.
 *
 * @author Andrey Lavrov
 */
public class EntityStorageQueryFactory<I> {

    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final String tableName;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource    instance of {@link DataSourceWrapper}
     * @param entityClass   entity class of corresponding {@link RecordStorage} instance
     */
    public EntityStorageQueryFactory(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> entityClass) {
        this.idColumn = IdColumn.newInstance(entityClass);
        this.dataSource = dataSource;
        this.tableName = DbTableNameFactory.newTableName(entityClass);
    }

    /** Returns a query that creates a new {@link EntityTable} if it does not exist. */
    public CreateEntityTableQuery newCreateEntityTableQuery() {
        final CreateEntityTableQuery.Builder<I> builder = CreateEntityTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setTableName(tableName);
        return builder.build();
    }

    /**
     * Returns a query that inserts a new {@link EntityStorageRecord} to the {@link EntityTable}.
     *
     * @param id        new entity record id
     * @param record    new entity record
     */
    public InsertEntityQuery newInsertEntityQuery(I id, EntityStorageRecord record) {
        final InsertEntityQuery.Builder<I> builder = InsertEntityQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setId(id)
                .setIdColumn(idColumn)
                .setRecord(record);
        return builder.build();
    }

    /**
     * Returns a query that updates {@link EntityStorageRecord} in the {@link EntityTable}.
     *
     * @param id        entity id
     * @param record    updated record state
     */
    public UpdateEntityQuery newUpdateEntityQuery(I id, EntityStorageRecord record) {
        final UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record);
        return builder.build();
    }

    /** Returns a query that selects {@link EntityStorageRecord} by ID. */
    public SelectEntityByIdQuery <I> newSelectEntityByIdQuery(I id){
        final SelectEntityByIdQuery.Builder<I> builder = SelectEntityByIdQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id);
        return builder.build();
    }

    /** Returns a query that deletes all from {@link EntityTable}. */
    public DeleteAllQuery newDeleteAllQuery(){
        final DeleteAllQuery.Builder builder = DeleteAllQuery.newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(logger);
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <M extends Message> SelectBulkQuery newSelectAllQuery(FieldMask fieldMask, Descriptors.Descriptor descriptor) {
        final SelectBulkQuery.Builder builder = SelectBulkQuery.newBuilder(tableName)
                .setFieldMask(fieldMask)
                .setMessageDescriptor(descriptor)
                .setLogger(logger)
                .setDataSource(dataSource);

        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <M extends Message> SelectBulkQuery newSelectBulkQuery(Iterable<?> ids, FieldMask fieldMask, Descriptors.Descriptor descriptor) {
        final SelectBulkQuery.Builder builder = SelectBulkQuery.newBuilder()
                .setIdsQuery(tableName, ids)
                .setFieldMask(fieldMask)
                .setMessageDescriptor(descriptor)
                .setLogger(logger)
                .setDataSource(dataSource);

        return builder.build();
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }
}
