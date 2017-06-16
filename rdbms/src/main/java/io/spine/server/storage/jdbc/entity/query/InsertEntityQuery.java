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

package io.spine.server.storage.jdbc.entity.query;

import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.query.WriteRecordQuery;
import io.spine.server.storage.jdbc.table.entity.RecordTable;
import io.spine.server.entity.EntityRecord;

import static io.spine.server.storage.jdbc.Sql.getColumnNames;
import static java.lang.String.format;
import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static io.spine.server.storage.jdbc.Sql.Query.VALUES;
import static io.spine.server.storage.jdbc.Sql.nPlaceholders;
import static io.spine.server.storage.jdbc.table.entity.RecordTable.Column;

/**
 * Query that inserts a new {@link EntityRecordWithColumns} to
 * the {@link RecordTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class InsertEntityQuery<I> extends WriteEntityQuery<I> {

    private static final String QUERY_TEMPLATE =
            INSERT_INTO + " %s " +
            BRACKET_OPEN +
            Column.entity + COMMA + " %s " + Column.id +
            BRACKET_CLOSE +
            VALUES + " %s " + SEMICOLON;

    private InsertEntityQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I> Builder<I> newBuilder(String tableName, EntityRecordWithColumns record) {
        final Builder<I> builder = new Builder<>();
        final int columnCount = record.getColumns().size() + 2;
        final String columnNames = getColumnNames(record);
        final String placeholders = nPlaceholders(columnCount);
        final String query = format(QUERY_TEMPLATE, tableName, columnNames, placeholders);
        builder.setIdIndexInQuery(columnCount)
               .setRecordIndexInQuery(QueryParameter.RECORD.index)
               .setQuery(query);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I>
            extends WriteRecordQuery.Builder<Builder<I>, InsertEntityQuery, I, EntityRecordWithColumns> {

        @Override
        public InsertEntityQuery build() {
            return new InsertEntityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
