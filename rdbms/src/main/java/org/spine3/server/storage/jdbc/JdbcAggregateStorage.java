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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.query.constants.AggregateTable;
import org.spine3.server.storage.jdbc.query.tables.aggregate.*;
import org.spine3.server.storage.jdbc.util.*;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static java.lang.String.format;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.io.IoUtil.closeAll;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the aggregate storage based on the RDBMS.
 *
 * @param <Id> the type of aggregate IDs
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcAggregateStorage<Id> extends AggregateStorage<Id> {

    private final DataSourceWrapper dataSource;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    private final IdColumn<Id> idColumn;

    private final String mainTableName;
    private final String eventCountTableName;


    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param aggregateClass the class of aggregates to save to the storage
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ static <ID> JdbcAggregateStorage<ID> newInstance(DataSourceWrapper dataSource,
                                                                 Class<? extends Aggregate<ID, ?, ?>> aggregateClass,
                                                                 boolean multitenant)
                                                                 throws DatabaseException {
        return new JdbcAggregateStorage<>(dataSource, aggregateClass, multitenant);
    }

    private JdbcAggregateStorage(DataSourceWrapper dataSource, Class<? extends Aggregate<Id, ?, ?>> aggregateClass, boolean multitenant)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + AggregateTable.EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.idColumn = IdColumn.newInstance(aggregateClass);
        CreateMainTableIfDoesNotExistQuery.getBuilder()
                .setTableName(mainTableName)
                .setIdType(idColumn.getColumnDataType())
                .setDataSource(dataSource)
                .build()
                .execute();
        CreateEventCountTableIfDoesNotExistQuery.getBuilder()
                .setTableName(eventCountTableName)
                .setIdType(idColumn.getColumnDataType())
                .setDataSource(dataSource)
                .build()
                .execute();
    }

    @Override
    public int readEventCountAfterLastSnapshot(Id id) {
        checkNotClosed();
        final Integer count = SelectEventCountByIdQuery.<Id>getBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setDataSource(dataSource)
                .build()
                .execute();
        if (count == null) {
            return 0;
        }
        return count;
    }

    @Override
    public void writeEventCountAfterLastSnapshot(Id id, int count) {
        checkNotClosed();
        if (containsEventCount(id)) {
            UpdateEventCountQuery.<Id>getBuilder(eventCountTableName)
                    .setIdColumn(idColumn)
                    .setId(id)
                    .setCount(count)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
        } else {
            InsertEventCountQuery.<Id>getBuilder(eventCountTableName)
                    .setIdColumn(idColumn)
                    .setId(id)
                    .setCount(count)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
        }
    }

    private boolean containsEventCount(Id id) {
        final Integer count = SelectEventCountByIdQuery.<Id>getBuilder(eventCountTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setDataSource(dataSource)
                .build()
                .execute();
        final boolean contains = count != null;
        return contains;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(Id id, AggregateStorageRecord record) throws DatabaseException {
        InsertRecordQuery.<Id>getBuilder(mainTableName)
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record)
                .setDataSource(dataSource)
                .build()
                .execute();
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before {@link Iterator#next()}.
     *
     * @return a new {@link DbIterator} instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected Iterator<AggregateStorageRecord> historyBackward(Id id) throws DatabaseException {
        checkNotNull(id);
            final ResultSet resultSet = SelectByIdSortedByTimeDescQuery.<Id>getBuilder(mainTableName)
                    .setIdColumn(idColumn)
                    .setId(id)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
            final DbIterator<AggregateStorageRecord> iterator = new DbIterator<>(resultSet, AggregateTable.AGGREGATE_COL, AggregateTable.RECORD_DESCRIPTOR);
            iterators.add(iterator);
            return iterator;
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
        private final Logger value = LoggerFactory.getLogger(JdbcAggregateStorage.class);
    }
}
