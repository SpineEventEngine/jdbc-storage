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
import org.spine3.base.Event;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumnSetter;

/**
 * This class creates queries for interaction with
 * {@link org.spine3.server.storage.jdbc.table.EventTable}.
 *
 * @author Andrey Lavrov
 */
public class EventTableQueryFactory implements QueryFactory<String, Event> {

    private final DataSourceWrapper dataSource;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     */
    public EventTableQueryFactory(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /** Returns a query that selects {@link Event} by specified {@link EventStreamQuery}. */
    public FilterAndSortQuery newFilterAndSortQuery(EventStreamQuery streamQuery) {
        final FilterAndSortQuery.Builder builder = FilterAndSortQuery.newBuilder()
                                                                     .setDataSource(dataSource)
                                                                     .setLogger(logger)
                                                                     .setStreamQuery(streamQuery);
        return builder.build();
    }

    @Override
    public SelectByIdQuery<String, Event> newSelectByIdQuery(String id) {
        final SelectEventByIdQuery.Builder builder =
                SelectEventByIdQuery.newBuilder()
                                    .setDataSource(dataSource)
                                    .setLogger(logger)
                                    .setIdColumnSetter(new IdColumnSetter.StringIdColumnSetter())
                                    .setId(id);
        return builder.build();
    }

    @Override
    public WriteQuery newInsertQuery(String id, Event record) {
        final InsertEventRecordQuery.Builder builder =
                InsertEventRecordQuery.newBuilder()
                                      .setDataSource(dataSource)
                                      .setLogger(logger)
                                      .setId(id)
                                      .setRecord(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(String id, Event record) {
        final UpdateEventRecordQuery.Builder builder =
                UpdateEventRecordQuery.newBuilder()
                                      .setDataSource(dataSource)
                                      .setLogger(logger)
                                      .setId(id)
                                      .setRecord(record);
        return builder.build();
    }
}
