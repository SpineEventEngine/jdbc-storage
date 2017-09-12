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

package io.spine.server.storage.jdbc.query;

import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.RecordTable;
import io.spine.server.storage.jdbc.RecordTable.StandardColumn;

import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.entity;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.id;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static io.spine.server.storage.jdbc.Sql.Query.VALUES;
import static io.spine.server.storage.jdbc.Sql.nPlaceholders;
import static java.lang.String.format;

/**
 * Query that inserts a new {@link EntityRecordWithColumns} to
 * the {@link RecordTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 * @author Alexander Aleksandrov
 */
class InsertEntityQuery<I> extends WriteEntityQuery<I> {

    private static final int ID_INDEX = 1;
    private static final int RECORD_INDEX = 2;

    private static final String FORMAT_PLACEHOLDER = "%s";
    private static final String COLUMN_FORMAT = COMMA + FORMAT_PLACEHOLDER;

    private static final String QUERY_TEMPLATE =
            INSERT_INTO + FORMAT_PLACEHOLDER +
            BRACKET_OPEN +
            id + COMMA + entity +
            FORMAT_PLACEHOLDER +
            BRACKET_CLOSE +
            VALUES + FORMAT_PLACEHOLDER + SEMICOLON;

    private InsertEntityQuery(Builder<I> builder) {
        super(builder);
    }

    static <I> Builder<I> newBuilder(String tableName, EntityRecordWithColumns record) {
        final Builder<I> builder = new Builder<>();
        final int columnCount = StandardColumn.values().length + record.getColumnNames()
                                                                       .size();
        final String columnList = formatAndMergeColumns(record, COLUMN_FORMAT);
        final String valuePlaceholders = nPlaceholders(columnCount);
        final String sqlQuery = format(QUERY_TEMPLATE,
                                       tableName,
                                       columnList,
                                       valuePlaceholders);
        builder.setIdIndexInQuery(ID_INDEX)
               .setRecordIndexInQuery(RECORD_INDEX)
               .setQuery(sqlQuery);
        return builder;
    }

    @Override
    protected int getFirstColumnIndex() {
        return RECORD_INDEX + 1;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends WriteRecordQuery.Builder<Builder<I>,
                                                             InsertEntityQuery,
                                                             I,
                                                             EntityRecordWithColumns> {

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
