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

package org.spine3.server.storage.jdbc.event.query;

import org.slf4j.Logger;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.event.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.event.query.EventTable.TABLE_NAME;

/**
 * This class creates queries for interaction with {@link EventTable}.
 *
 * @author Andrey Lavrov
 */
public class EventStorageQueryFactory {

    private final DataSourceWrapper dataSource;
    private final IdColumn<String> idColumn;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     */
    public EventStorageQueryFactory(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
        this.idColumn = new IdColumn.StringIdColumn();
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /** Returns a query that creates a new {@link EventTable} if it does not exist. */
    public CreateEventTableQuery newCreateEventTableQuery() {
        final CreateEventTableQuery.Builder builder = CreateEventTableQuery.newBuilder()
                                                                           .setDataSource(
                                                                                   dataSource)
                                                                           .setLogger(logger)
                                                                           .setIdColumn(idColumn)
                                                                           .setTableName(
                                                                                   TABLE_NAME);
        return builder.build();
    }

    /**
     * Returns a query that inserts a new {@link EventStorageRecord} to the {@link EventTable}.
     *
     * @param record new event record
     */
    public InsertEventRecordQuery newInsertEventQuery(EventStorageRecord record) {
        final InsertEventRecordQuery.Builder builder = InsertEventRecordQuery.newBuilder()
                                                                             .setDataSource(
                                                                                     dataSource)
                                                                             .setLogger(logger)
                                                                             .setRecord(record);
        return builder.build();
    }

    /**
     * Returns a query that updates {@link EventStorageRecord} in the {@link EventTable}.
     *
     * @param record updated record state
     */
    public UpdateEventRecordQuery newUpdateEventQuery(EventStorageRecord record) {
        final UpdateEventRecordQuery.Builder builder = UpdateEventRecordQuery.newBuilder()
                                                                             .setDataSource(
                                                                                     dataSource)
                                                                             .setLogger(logger)
                                                                             .setRecord(record);
        return builder.build();
    }

    /** Returns a query that selects {@link EventStorageRecord} by ID. */
    public SelectEventByIdQuery newSelectEventByIdQuery(String id) {
        final SelectEventByIdQuery.Builder builder = SelectEventByIdQuery.newBuilder()
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger)
                                                                         .setIdColumn(idColumn)
                                                                         .setId(id);
        return builder.build();
    }

    /** Returns a query that selects {@link EventStorageRecord} by specified {@link EventStreamQuery}. */
    public FilterAndSortQuery newFilterAndSortQuery(EventStreamQuery streamQuery) {
        final FilterAndSortQuery.Builder builder = FilterAndSortQuery.newBuilder()
                                                                     .setDataSource(dataSource)
                                                                     .setLogger(logger)
                                                                     .setStreamQuery(streamQuery);
        return builder.build();
    }
}
