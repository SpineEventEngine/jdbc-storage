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

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.protobuf.Messages;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.query.constants.EventTable;
import org.spine3.server.storage.jdbc.query.tables.event.*;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;
import org.spine3.server.storage.jdbc.util.IdColumn.StringIdColumn;
import org.spine3.server.storage.jdbc.util.SelectByIdQuery;
import org.spine3.server.storage.jdbc.util.WriteQuery;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newLinkedList;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.io.IoUtil.closeAll;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the event storage based on the RDBMS.
 *
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcEventStorage extends EventStorage {

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    /**
     * Creates a new storage instance.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     * @param dataSource the dataSource wrapper
     */
    /*package*/ static JdbcEventStorage newInstance(DataSourceWrapper dataSource, boolean multitenant) throws DatabaseException {
        return new JdbcEventStorage(dataSource, multitenant);
    }

    private JdbcEventStorage(DataSourceWrapper dataSource, boolean multitenant) throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        CreateTableIfDoesNotExistQuery.getBuilder()
                .setDataSource(dataSource)
                .build()
                .execute();
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before {@link Iterator#next()}.
     *
     * @return a wrapped {@link DbIterator} instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public Iterator<Event> iterator(EventStreamQuery query) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final ResultSet resultSet = FilterAndSortQuery.getBuilder().setStreamQuery(query).setDataSource(dataSource).build().execute();
            final DbIterator<EventStorageRecord> iterator = new DbIterator<>(resultSet, EventTable.EVENT_COL, EventTable.RECORD_DESCRIPTOR);
            iterators.add(iterator);
            final Iterator<Event> result = toEventIterator(iterator);
            return result;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(EventStorageRecord record) throws DatabaseException {
        if (containsRecord(record.getEventId())) {
            UpdateEventQuery.getBuilder()
                    .setRecord(record)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
        } else {
            InsertEventQuery.getBuilder()
                    .setRecord(record)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    protected EventStorageRecord readInternal(EventId eventId) throws DatabaseException {
        final String id = eventId.getUuid();
        final EventStorageRecord record = new SelectEventByIdQuery(dataSource, id).execute();
        return record;
    }

    private boolean containsRecord(String id) {
        final EventStorageRecord record = new SelectEventByIdQuery(dataSource, id).execute();
        final boolean contains = record != null;
        return contains;
    }

    @Override
    public void close() throws DatabaseException {
        closeAll(iterators);
        iterators.clear();
        dataSource.close();
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
        private final Logger value = LoggerFactory.getLogger(JdbcEventStorage.class);
    }
}
