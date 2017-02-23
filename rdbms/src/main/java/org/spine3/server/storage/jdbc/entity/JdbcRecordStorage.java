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

package org.spine3.server.storage.jdbc.entity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.FieldMask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.query.SelectBulkQuery;
import org.spine3.server.storage.jdbc.query.DeleteRowQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @param <I> the type of entity IDs
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
public class JdbcRecordStorage<I> extends RecordStorage<I> {


    private final DataSourceWrapper dataSource;

    private final RecordStorageQueryFactory<I> queryFactory;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource   the dataSource wrapper
     * @param multitenant  defines is this storage multitenant
     * @param queryFactory factory that generates queries for interaction with entity table
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    public static <I> JdbcRecordStorage<I> newInstance(DataSourceWrapper dataSource,
                                                       boolean multitenant,
                                                       RecordStorageQueryFactory<I> queryFactory)
            throws DatabaseException {
        return new JdbcRecordStorage<>(dataSource, multitenant, queryFactory);
    }

    protected JdbcRecordStorage(DataSourceWrapper dataSource, boolean multitenant,
                                RecordStorageQueryFactory<I> queryFactory)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.queryFactory = queryFactory;
        queryFactory.setLogger(LogSingleton.INSTANCE.value);
        queryFactory.newCreateEntityTableQuery()
                    .execute();
    }

    @SuppressWarnings("ProhibitedExceptionThrown") // NPE by the contract
    @Override
    public void markArchived(I id) {
        checkNotNull(id);
        final boolean recordExists = queryFactory.newMarkArchivedQuery(id)
                    .execute();
        if (!recordExists) {
            throw new NullPointerException(
                    String.format("Trying to mark not existing record with id %s archived.", id));
        }
    }

    @SuppressWarnings("ProhibitedExceptionThrown") // NPE by the contract
    @Override
    public void markDeleted(I id) {
        checkNotNull(id);
        final boolean recordExists = queryFactory.newMarkDeletedQuery(id)
                    .execute();
        if (!recordExists) {
            throw new NullPointerException(
                    String.format("Trying to mark not existing record with id %s deleted.", id));
        }
    }

    @Override
    public boolean delete(I id) {
        checkNotNull(id);

        final DeleteRowQuery<I> query = queryFactory.newDeleteRowQuery(id);
        final boolean result = query.execute();
        return result;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Optional<EntityRecord> readRecord(I id) throws DatabaseException {
        final EntityRecord record = queryFactory.newSelectEntityByIdQuery(id)
                                                .execute();
        return Optional.fromNullable(record);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return readMultipleRecords(ids, FieldMask.getDefaultInstance());
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                         FieldMask fieldMask) {
        final SelectBulkQuery<I> query = queryFactory.newSelectBulkQuery(ids, fieldMask);
        final Map<?, EntityRecord> recordMap;
        try {
            recordMap = query.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return ImmutableList.copyOf(recordMap.values());
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords() {
        return readAllRecords(FieldMask.getDefaultInstance());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Map<I, EntityRecord> readAllRecords(FieldMask fieldMask) {
        final SelectBulkQuery query = queryFactory.newSelectAllQuery(fieldMask);
        final Map<I, EntityRecord> records;

        try {
            records = (Map<I, EntityRecord>) query.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return ImmutableMap.copyOf(records);
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @VisibleForTesting
    @Override
    protected void writeRecord(I id, EntityRecord record) throws DatabaseException {
        checkArgument(record.hasState(), "entity state");

        if (containsRecord(id)) {
            queryFactory.newUpdateEntityQuery(id, record)
                        .execute();
        } else {
            queryFactory.newInsertEntityQuery(id, record)
                        .execute();
        }
    }

    @Override
    protected void writeRecords(Map<I, EntityRecord> records) {
        // Map's initial capacity is maximum meaning no records exist in the storage yet
        final Map<I, EntityRecord> newRecords = new HashMap<>(records.size());

        for (Map.Entry<I, EntityRecord> unclassifiedRecord : records.entrySet()) {
            final I id = unclassifiedRecord.getKey();
            final EntityRecord record = unclassifiedRecord.getValue();
            if (containsRecord(id)) {
                queryFactory.newUpdateEntityQuery(id, record)
                            .execute();
            } else {
                newRecords.put(id, record);
            }
        }
        queryFactory.newInsertEntityRecordsBulkQuery(newRecords)
                    .execute();
    }

    private boolean containsRecord(I id) throws DatabaseException {
        final EntityRecord record = queryFactory.newSelectEntityByIdQuery(id)
                                                .execute();
        final boolean contains = record != null;
        return contains;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            super.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        dataSource.close();
    }

    /**
     * Clears all data in the storage.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    void clear() throws DatabaseException {
        queryFactory.newDeleteAllQuery()
                    .execute();
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcRecordStorage.class);
    }
}
