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

package org.spine3.server.storage.jdbc.projection;

import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.ProjectionStorage;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.entity.JdbcEntityStorage;
import org.spine3.server.storage.jdbc.projection.query.ProjectionStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import javax.annotation.Nullable;

import static java.lang.String.format;

/**
 * The implementation of the projection storage based on the RDBMS.
 *
 * @param <I> a type of projection IDs
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
public class JdbcProjectionStorage<I> extends ProjectionStorage<I> {

    private final JdbcEntityStorage<I> entityStorage;

    private final ProjectionStorageQueryFactory queryFactory;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource      a data source used by an {@code entityStorage}
     * @param entityStorage   an entity storage to use
     * @param <I>            a type of projection IDs
     * @return a new storage instance
     */
    public static <I> ProjectionStorage<I> newInstance(DataSourceWrapper dataSource,
                                                JdbcEntityStorage<I> entityStorage,
                                                boolean multitenant,
                                                ProjectionStorageQueryFactory<I> queryFactory) throws DatabaseException {
        return new JdbcProjectionStorage<>(dataSource, entityStorage, multitenant, queryFactory);
    }

    private JdbcProjectionStorage(DataSourceWrapper dataSource,
                                  JdbcEntityStorage<I> entityStorage,
                                  boolean multitenant,
                                  ProjectionStorageQueryFactory<I> queryFactory) throws DatabaseException {
        super(multitenant);
        this.entityStorage = entityStorage;
        this.queryFactory = queryFactory;

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
    public EntityStorage<I> getEntityStorage() {
        return entityStorage;
    }

    @Override
    public void close() throws DatabaseException {
        // close only entityStorage because it must close dataSource itself
        entityStorage.close();
        try {
            super.close();
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcProjectionStorage.class);
    }
}
