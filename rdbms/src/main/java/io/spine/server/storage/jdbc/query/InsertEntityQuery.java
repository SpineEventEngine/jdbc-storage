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

import com.google.common.base.Joiner;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.RecordTable;
import io.spine.server.storage.jdbc.RecordTable.StandardColumn;

import java.util.Map;

import static io.spine.server.entity.storage.EntityColumns.sorted;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static io.spine.server.storage.jdbc.Sql.Query.VALUES;
import static io.spine.server.storage.jdbc.Sql.nPlaceholders;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.entity;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.id;
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
        final int columnCount;
        final String entityColumnNames;
        if (record.hasColumns()) {
            final Map<String, Column> columns = record.getColumns();
            columnCount = columns.size() + StandardColumn.values().length;
            entityColumnNames = COMMA + Joiner.on(COMMA.toString())
                                              .join(sorted(columns.keySet()));
        } else {
            columnCount = StandardColumn.values().length;
            entityColumnNames = "";
        }
        final String valuePlaceholders = nPlaceholders(columnCount);
        final String sqlQuery = format(QUERY_TEMPLATE,
                                       tableName,
                                       entityColumnNames,
                                       valuePlaceholders);
        builder.setIdIndexInQuery(ID_INDEX)
               .setRecordIndexInQuery(RECORD_INDEX)
               .setQuery(sqlQuery);
        return builder;
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
