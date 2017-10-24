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
import com.google.common.base.Predicates;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.RecordTable.StandardColumn;
import io.spine.server.storage.jdbc.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Maps.newHashMap;
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
import static java.util.Collections.nCopies;

/**
 * A query for {@code INSERT}-ing multiple {@linkplain EntityRecord entity records} as a bulk.
 *
 * @author Dmytro Dashenkov
 */
class InsertEntityRecordsBulkQuery<I> extends ColumnAwareWriteQuery {

    private static final String FORMAT_PLACEHOLDER = "%s";

    private static final String SQL_TEMPLATE =
            INSERT_INTO + FORMAT_PLACEHOLDER +
            BRACKET_OPEN + id + COMMA +
            entity +
            FORMAT_PLACEHOLDER +
            BRACKET_CLOSE +
            VALUES + FORMAT_PLACEHOLDER + SEMICOLON;

    private final Map<I, EntityRecordWithColumns> records;
    private final IdColumn<I, ?> idColumn;

    private final int columnCount;

    InsertEntityRecordsBulkQuery(Builder<I> builder) {
        super(builder);
        this.records = builder.records;
        this.idColumn = builder.idColumn;
        this.columnCount = builder.columnCount;
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @Override
    protected IdentifiedParameters getQueryParameters() {
        final IdentifiedParameters superParameters = super.getQueryParameters();
        final IdentifiedParameters.Builder builder =
                IdentifiedParameters.newBuilder()
                                    .addParameters(superParameters);
        int columnIndex = 1;
        for (Map.Entry<I, EntityRecordWithColumns> recordPair : records.entrySet()) {
            final I id = recordPair.getKey();
            final EntityRecordWithColumns record = recordPair.getValue();
            addRecordParams(builder, columnIndex, id, record);
            if (record.hasColumns()) {
                final int nextIndex = columnIndex + StandardColumn.values().length;
                ColumnRecords.feedColumnsTo(builder,
                                            record,
                                            getColumnTypeRegistry(),
                                            getEntityColumnIdentifier(record, nextIndex));
            }
            columnIndex += columnCount;
        }
        return builder.build();
    }

    /**
     * Adds an {@link EntityRecordWithColumns} to the query parameters.
     *
     * <p>This includes following items:
     * <ul>
     *     <li>ID;
     *     <li>serialized record by itself;
     * </ul>
     *
     * @param parametersBuilder the builder to add the query parameters to
     * @param firstParamIndex   index of the first query parameter which must be assigned
     * @param id                the ID of the record
     * @param record            the record to store
     */
    private void addRecordParams(IdentifiedParameters.Builder parametersBuilder,
                                 int firstParamIndex,
                                 I id,
                                 EntityRecordWithColumns record) {
        int paramIndex = firstParamIndex;
        idColumn.setId(paramIndex, id, parametersBuilder);
        paramIndex++;
        final byte[] serializedRecord = Serializer.serialize(record.getRecord());
        parametersBuilder.addParameter(paramIndex, serializedRecord);
    }

    static class Builder<I> extends ColumnAwareWriteQuery.Builder<Builder<I>,
                                                                  InsertEntityRecordsBulkQuery> {

        private static final String COLUMN_FORMAT = COMMA + FORMAT_PLACEHOLDER;

        private final Map<I, EntityRecordWithColumns> records = newHashMap();
        private String tableName;
        private IdColumn<I, ?> idColumn;
        private int columnCount;

        Builder<I> setRecords(Map<I, EntityRecordWithColumns> records) {
            checkNotNull(records);
            this.records.putAll(records);
            return getThis();
        }

        Builder<I> setTableName(String tableName) {
            checkArgument(!isNullOrEmpty(tableName));
            this.tableName = tableName;
            return getThis();
        }

        Builder<I> setIdColumn(IdColumn<I, ?> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return getThis();
        }

        @Override
        public InsertEntityRecordsBulkQuery<I> build() {
            if (records.isEmpty()) {
                throw new IllegalStateException("Records are not set.");
            }
            final EntityRecordWithColumns gaugeRecord =
                    find(records.values(), Predicates.<EntityRecordWithColumns>notNull());
            columnCount = StandardColumn.values().length + gaugeRecord.getColumnNames().size();
            final Collection<String> sqlValues = nCopies(records.size(),
                                                         nPlaceholders(columnCount));
            final String entityColumnNames = formatAndMergeColumns(gaugeRecord, COLUMN_FORMAT);
            final String sqlValuesJoined = Joiner.on(COMMA.toString())
                                                 .join(sqlValues);
            final String sql = format(SQL_TEMPLATE, tableName, entityColumnNames, sqlValuesJoined);
            setQuery(sql);
            return new InsertEntityRecordsBulkQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
