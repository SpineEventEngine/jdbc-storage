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

package org.spine3.server.storage.jdbc.projection;

import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.projection.ProjectionStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.entity.JdbcRecordStorage;
import org.spine3.server.storage.jdbc.projection.query.ProjectionStorageQueryFactory;

import javax.annotation.Nullable;
import java.util.Map;


/**
 * The implementation of the projection storage based on the RDBMS.
 *
 * @param <I> a type of projection IDs
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
public class JdbcProjectionStorage<I> extends ProjectionStorage<I> {

    private final JdbcRecordStorage<I> entityStorage;

    private final ProjectionStorageQueryFactory queryFactory;

    /**
     * Creates a new storage instance.
     *
     * @param entityStorage   an entity storage to use
     * @param multitenant     defines is this storage multitenant
     * @param queryFactory    factory that will generate queries for interaction with projection table
     * @param <I>             a type of projection IDs
     * @return a new storage instance
     */
    public static <I> ProjectionStorage<I> newInstance(JdbcRecordStorage<I> entityStorage,
                                                       boolean multitenant,
                                                       ProjectionStorageQueryFactory<I> queryFactory)
            throws DatabaseException {
        return new JdbcProjectionStorage<>(entityStorage, multitenant, queryFactory);
    }

    protected JdbcProjectionStorage(JdbcRecordStorage<I> entityStorage,
                                    boolean multitenant,
                                    ProjectionStorageQueryFactory<I> queryFactory) throws DatabaseException {
        super(multitenant);
        this.entityStorage = entityStorage;
        this.queryFactory = queryFactory;
        queryFactory.setLogger(LogSingleton.INSTANCE.value);

       queryFactory.newCreateTableQuery().execute();
    }

    @Override
    public void writeLastHandledEventTime(Timestamp time) throws DatabaseException {
        if (containsLastEventTime()) {
           queryFactory.newUpdateTimestampQuery(time).execute();
        } else {
            queryFactory.newInsertTimestampQuery(time).execute();
        }
    }

    private boolean containsLastEventTime() throws DatabaseException {
        final Timestamp time = readLastHandledEventTime();
        final boolean containsEventTime = time != null;
        return containsEventTime;
    }

    @Override
    @Nullable
    public Timestamp readLastHandledEventTime() throws DatabaseException {
        final Timestamp timestamp = queryFactory.newSelectTimestampQuery().execute();
        return timestamp;
    }

    @Override
    public RecordStorage<I> recordStorage() {
        return entityStorage;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            super.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        // close only entityStorage because it must close dataSource by itself
        entityStorage.close();
    }

    @Override
    public boolean markArchived(I id) {
        return false;
    }

    @Override
    public boolean markDeleted(I id) {
        return false;
    }

    @Override
    public boolean delete(I id) {
        return false;
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids) {
        return entityStorage.readMultiple(ids);
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids, FieldMask fieldMask) {
        return entityStorage.readMultiple(ids, fieldMask);
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords() {
        return entityStorage.readAll();
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords(FieldMask fieldMask) {
        return entityStorage.readAll(fieldMask);
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcProjectionStorage.class);
    }
}
