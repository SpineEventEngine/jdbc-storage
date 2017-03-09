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

package org.spine3.server.storage.jdbc.table;

import org.spine3.base.Event;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.jdbc.Sql.Type;
import org.spine3.server.storage.jdbc.event.query.EventStorageQueryFactory;
import org.spine3.server.storage.jdbc.event.query.FilterAndSortQuery;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.util.Iterator;

import static org.spine3.server.storage.jdbc.Sql.Type.*;

/**
 * @author Dmytro Dashenkov.
 */
public class EventTable extends AbstractTable<String, Event, EventTable.Column> {

    private static final String TABLE_NAME = "events";

    private final EventStorageQueryFactory queryFactory;

    public EventTable(DataSourceWrapper dataSource) {
        super(TABLE_NAME, new IdColumn.StringIdColumn(), dataSource);
        this.queryFactory = new EventStorageQueryFactory(dataSource);
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.event_id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    @Override
    protected QueryFactory<String, Event> getQueryFactory() {
        return queryFactory;
    }

    public Iterator<Event> getEventStream(EventStreamQuery query) {
        final FilterAndSortQuery sqlQuery = queryFactory.newFilterAndSortQuery(query);
        final Iterator<Event> result = sqlQuery.execute();
        return result;
    }

    enum Column implements TableColumn {

        event_id(VARCHAR_999),
        event(BLOB),
        event_type(VARCHAR_512),
        producer_id(VARCHAR_512),
        seconds(BIGINT),
        nanoseconds(INT);

        private final Type type;

        Column(Type type) {
            this.type = type;
        }

        @Override
        public Type type() {
            return type;
        }
    }
}