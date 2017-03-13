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

import com.google.protobuf.Timestamp;
import org.spine3.server.storage.jdbc.projection.query.LastHandledEventTimeQueryFactory;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.Sql.Type;
import static org.spine3.server.storage.jdbc.Sql.Type.BIGINT;
import static org.spine3.server.storage.jdbc.Sql.Type.INT;
import static org.spine3.server.storage.jdbc.Sql.Type.VARCHAR_255;

/**
 * A table for storing the last handled by
 * a {@link org.spine3.server.projection.ProjectionRepository} event time.
 *
 * @see org.spine3.server.projection.ProjectionRepository#catchUp
 * @author Dmytro Dashenkov
 */
public class LastHandledEventTimeTable extends AbstractTable<String,
                                                             Timestamp,
                                                             LastHandledEventTimeTable.Column> {

    private static final String TABLE_NAME = "projection_last_handled_event_time";

    private final LastHandledEventTimeQueryFactory queryFactory;

    public LastHandledEventTimeTable(DataSourceWrapper dataSource) {
        super(TABLE_NAME,
              IdColumn.typeString(Column.projection_type.name()),
              dataSource);
        this.queryFactory = new LastHandledEventTimeQueryFactory(dataSource, TABLE_NAME);
        queryFactory.setLogger(log());
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.projection_type;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    @Override
    protected QueryFactory<String, Timestamp> getQueryFactory() {
        return queryFactory;
    }

    public enum Column implements TableColumn {

        projection_type(VARCHAR_255),
        seconds(BIGINT),
        nanos(INT);

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
