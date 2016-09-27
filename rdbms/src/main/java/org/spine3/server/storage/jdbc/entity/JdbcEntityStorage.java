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

package org.spine3.server.storage.jdbc.entity;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.FieldMask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.entity.query.EntityStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @param <I> the type of entity IDs
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
public class JdbcEntityStorage<I> extends RecordStorage<I> {

    private final DataSourceWrapper dataSource;

    private final EntityStorageQueryFactory<I> queryFactory;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource            the dataSource wrapper
     * @param multitenant           defines is this storage multitenant
     * @param queryFactory          factory that generates queries for interaction with entity table
     * @throws DatabaseException    if an error occurs during an interaction with the DB
     */
    public static <I> JdbcEntityStorage<I> newInstance(DataSourceWrapper dataSource,
                                                       boolean multitenant,
                                                       EntityStorageQueryFactory<I> queryFactory)
            throws DatabaseException {
        return new JdbcEntityStorage<>(dataSource, multitenant, queryFactory);
    }

    protected JdbcEntityStorage(DataSourceWrapper dataSource, boolean multitenant, EntityStorageQueryFactory<I> queryFactory)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.queryFactory = queryFactory;
        queryFactory.setLogger(LogSingleton.INSTANCE.value);
        queryFactory.newCreateEntityTableQuery().execute();
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    protected EntityStorageRecord readInternal(I id) throws DatabaseException {
        final EntityStorageRecord record = queryFactory.newSelectEntityByIdQuery(id).execute();
        return record;
    }

    // TODO:27-09-16:dmytro.dashenkov: Implement.
    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<I> ids) {
        return null;
    }

    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<I> ids, FieldMask fieldMask) {
        return null;
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllInternal() {
        return null;
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllInternal(FieldMask fieldMask) {
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @VisibleForTesting
    @Override
    protected void writeInternal(I id, EntityStorageRecord record) throws DatabaseException {
        checkArgument(record.hasState(), "entity state");

        if (containsRecord(id)) {
            queryFactory.newUpdateEntityQuery(id, record).execute();
        } else {
            queryFactory.newInsertEntityQuery(id, record).execute();
        }
    }

    private boolean containsRecord(I id) throws DatabaseException {
        final EntityStorageRecord record = queryFactory.newSelectEntityByIdQuery(id).execute();
        final boolean contains = record != null;
        return contains;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            super.close();
        } catch (Exception e) {
            throw propagate(e);
        }
        dataSource.close();
    }

    /**
     * Clears all data in the storage.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ void clear() throws DatabaseException {
       queryFactory.newDeleteAllQuery().execute();
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcEntityStorage.class);
    }
}
