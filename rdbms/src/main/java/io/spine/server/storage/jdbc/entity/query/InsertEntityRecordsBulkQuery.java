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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.query.ColumnAwareWriteQuery;
import io.spine.server.storage.jdbc.table.entity.RecordTable;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;
import io.spine.server.storage.jdbc.util.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

/**
 * A query for {@code INSERT}-ing multiple {@linkplain EntityRecord entity records} as a bulk.
 *
 * @author Dmytro Dashenkov
 */
public class InsertEntityRecordsBulkQuery<I> extends ColumnAwareWriteQuery {

    private static final String FORMAT_PLACEHOLDER = "%s";

    private static int columnsCount;

    private static final String SQL_TEMPLATE =
            Sql.Query.INSERT_INTO + FORMAT_PLACEHOLDER +
            BRACKET_OPEN + RecordTable.Column.id + COMMA +
            RecordTable.Column.entity + COMMA +
            FORMAT_PLACEHOLDER + BRACKET_CLOSE +
            Sql.Query.VALUES + FORMAT_PLACEHOLDER;

    private static final String SQL_VALUES_TEMPLATE = Sql.nPlaceholders(columnsCount);

    private final Map<I, EntityRecordWithColumns> records;
    private final IdColumn<I> idColumn;


    protected InsertEntityRecordsBulkQuery(Builder<I> builder) {
        super(builder);
        this.records = builder.records;
        this.idColumn = builder.idColumn;
        this.columnsCount = builder.columnsCount;
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        for (Map.Entry<I, EntityRecordWithColumns> record : records.entrySet()) {
            if (record.getValue().hasColumns()){
            ColumnRecords.feedColumnsTo(statement,
                                        record.getValue(),
                                        getColumnTypeRegistry(),
                                        getTransformer(record.getKey(), record.getValue()));
            }
//        int parameterCounter = 1;
//            final I id = record.getKey();
//            final EntityRecordWithColumns storageRecord = record.getValue();
//            addRecordsParams(statement, parameterCounter, id, storageRecord);
//            parameterCounter += COLUMNS_COUNT;
        }
        return statement;
    }

    protected Function<String, Integer> getTransformer(I id, EntityRecordWithColumns record) {
        final Function<String, Integer> function;
        final Map<String, Column> columns = record.getColumns();
        final List<String> columnList = Lists.newArrayList(columns.keySet());
        Collections.sort(columnList, Ordering.usingToString());
        final Map<String, Integer> result = Collections.emptyMap();

        Integer index = 2;

        for (String entry : columnList) {
            result.put(entry, index);
            index++;
        }

        function = Functions.forMap(result);
        return function;
    }

    /**
     * Adds an {@link EntityRecordWithColumns} to the query.
     *
     * <p>This includes following items:
     * <ul>
     *      <li>ID;
     *      <li>serialized record by itself;
     *      <li>{@code Boolean} columns for the record visibility (archived and deleted fields).
     * </ul>
     *
     * @param statement       the {@linkplain PreparedStatement} to add the query parameters to
     * @param firstParamIndex index of the first query parameter which must be assigned;
     *                        after this operation there will be added {@linkplain #COLUMNS_COUNT}
     *                        params starting with this index
     * @param id              the ID of the record
     * @param record          the record to store
     */
    private void addRecordsParams(PreparedStatement statement, int firstParamIndex, I id,
                                  EntityRecordWithColumns record) {
        int paramIndex = firstParamIndex;
        try {
            idColumn.setId(paramIndex, id, statement);
            paramIndex++;
            final byte[] bytes = Serializer.serialize(record.getRecord());
            statement.setBytes(paramIndex, bytes);
            paramIndex++;
            final LifecycleFlags status = record.getRecord()
                                                .getLifecycleFlags();
            final boolean archived = status.getArchived();
            final boolean deleted = status.getDeleted();
            statement.setBoolean(paramIndex, archived);
            paramIndex++;
            statement.setBoolean(paramIndex, deleted);
        } catch (SQLException e) {
            logWriteError(id, e);
            throw new DatabaseException(e);
        }
    }

    public static class Builder<I>
            extends ColumnAwareWriteQuery.Builder<Builder<I>, InsertEntityRecordsBulkQuery> {

        private Map<I, EntityRecordWithColumns> records;
        private String tableName;
        private IdColumn<I> idColumn;
        private int columnsCount = records.get(1).getColumns().size() + 2;


        public Builder<I> setQueryForBulkInsert() {
            checkState(!isNullOrEmpty(tableName), "Table name is not set.");
            checkState(records != null, "Records field is not set.");

            final Collection<String> sqlValues = nCopies(records.size(),
                                                         SQL_VALUES_TEMPLATE);
            final String sqlValuesJoined = Joiner.on(COMMA.toString())
                                                 .join(sqlValues);
            final String sql =
                    format(SQL_TEMPLATE, tableName, sqlValuesJoined) + SEMICOLON;
            setQuery(sql);
            return getThis();
        }

        public Builder<I> setRecords(Map<I, EntityRecordWithColumns> records) {
            this.records = checkNotNull(records);
            return getThis();
        }

        public Builder<I> setTableName(String tableName) {
            checkArgument(!isNullOrEmpty(tableName));
            this.tableName = tableName;
            return getThis();
        }

        public Builder<I> setidColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return getThis();
        }


        @Override
        public InsertEntityRecordsBulkQuery<I> build() {
            return new InsertEntityRecordsBulkQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
