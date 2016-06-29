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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.tables.entity.*;
import org.spine3.server.storage.jdbc.util.*;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.protobuf.Descriptors.Descriptor;
import static java.lang.String.format;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @param <Id> the type of entity IDs
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcEntityStorage<Id> extends EntityStorage<Id> {

    private final DataSourceWrapper dataSource;

    private final IdColumn<Id> idColumn;

    private final String tableName;


    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param entityClass the class of entities to save to the storage
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ static <Id> JdbcEntityStorage<Id> newInstance(DataSourceWrapper dataSource,
                                                              Class<? extends Entity<Id, ?>> entityClass,
                                                              boolean multitenant)
                                                              throws DatabaseException {
        return new JdbcEntityStorage<>(dataSource, entityClass, multitenant);
    }

    private JdbcEntityStorage(DataSourceWrapper dataSource, Class<? extends Entity<Id, ?>> entityClass, boolean multitenant)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.idColumn = IdColumn.newInstance(entityClass);
        this.tableName = DbTableNameFactory.newTableName(entityClass);

        CreateTableIfDoesNotExistQuery.getBuilder()
                .setDataSource(dataSource)
                .setIdType(idColumn.getColumnDataType())
                .setTableName(tableName)
                .build()
                .execute();
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    protected EntityStorageRecord readInternal(Id id) throws DatabaseException {
        final EntityStorageRecord record = new SelectEntityByIdQuery<>(tableName, dataSource, idColumn, id).execute();
        return record;
    }


    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(Id id, EntityStorageRecord record) throws DatabaseException {
        checkArgument(record.hasState(), "entity state");

        if (containsRecord(id)) {
            UpdateEntityQuery.<Id>getBuilder(tableName)
                    .setIdColumn(idColumn)
                    .setId(id)
                    .setRecord(record)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
        } else {
            InsertEntityQuery.<Id>getBuilder(tableName)
                    .setId(id)
                    .setIdColumn(idColumn)
                    .setRecord(record)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
        }
    }

    private boolean containsRecord(Id id) throws DatabaseException {
        final EntityStorageRecord record = new SelectEntityByIdQuery<>(tableName, dataSource, idColumn, id).execute();
        final boolean contains = record != null;
        return contains;
    }

    @Override
    public void close() throws DatabaseException {
        dataSource.close();
        try {
            super.close();
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Clears all data in the storage.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ void clear() throws DatabaseException {
        DeleteAllQuery.getBuilder(tableName)
                .setDataSource(dataSource)
                .build()
                .execute();
    }
    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcEntityStorage.class);
    }
}
