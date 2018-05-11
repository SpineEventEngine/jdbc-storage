/*
 * Copyright 2017, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.projection;

import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.AbstractTable;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static com.google.common.collect.ImmutableList.copyOf;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.PROJECTION_TYPE;
import static io.spine.server.storage.jdbc.query.IdColumn.typeString;

/**
 * A table for storing the last handled by
 * a {@link io.spine.server.projection.ProjectionRepository} event time.
 *
 * @author Dmytro Dashenkov
 */
class LastHandledEventTimeTable extends AbstractTable<String, Timestamp, Timestamp> {

    private static final String TABLE_NAME = "projection_last_handled_event_time";

    LastHandledEventTimeTable(DataSourceWrapper dataSource, TypeMapping typeMapping) {
        super(TABLE_NAME, typeString(PROJECTION_TYPE.name()), dataSource, typeMapping);
    }

    @Override
    protected Column getIdColumnDeclaration() {
        return PROJECTION_TYPE;
    }

    @Override
    protected List<? extends TableColumn> getTableColumns() {
        return copyOf(Column.values());
    }

    @Override
    protected SelectQuery<Timestamp> composeSelectQuery(String id) {
        final SelectTimestampQuery.Builder builder = SelectTimestampQuery.newBuilder();
        final SelectTimestampQuery query = builder.setTableName(getName())
                                                  .setDataSource(getDataSource())
                                                  .setId(id)
                                                  .setIdColumn(getIdColumn())
                                                  .build();
        return query;
    }

    @Override
    protected WriteQuery composeInsertQuery(String id, Timestamp record) {
        final InsertTimestampQuery.Builder builder = InsertTimestampQuery.newBuilder();
        final InsertTimestampQuery query = builder.setTableName(getName())
                                                  .setDataSource(getDataSource())
                                                  .setId(id)
                                                  .setTimestamp(record)
                                                  .build();
        return query;
    }

    @Override
    protected WriteQuery composeUpdateQuery(String id, Timestamp record) {
        final UpdateTimestampQuery.Builder builder = UpdateTimestampQuery.newBuilder();
        final UpdateTimestampQuery query = builder.setTableName(getName())
                                                  .setDataSource(getDataSource())
                                                  .setId(id)
                                                  .setTimestamp(record)
                                                  .build();
        return query;
    }

    /**
     * The enumeration of the columns of a {@link LastHandledEventTimeTable}.
     */
    enum Column implements TableColumn {

        PROJECTION_TYPE(STRING_255),
        SECONDS(LONG),
        NANOS(INT);

        private final Type type;

        Column(Type type) {
            this.type = type;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public boolean isPrimaryKey() {
            return this == PROJECTION_TYPE;
        }

        @Override
        public boolean isNullable() {
            return false;
        }
    }
}
