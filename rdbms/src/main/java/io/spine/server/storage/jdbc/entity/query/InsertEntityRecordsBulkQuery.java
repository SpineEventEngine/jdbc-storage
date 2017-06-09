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

import com.google.common.base.Joiner;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.table.entity.RecordTable;
import io.spine.server.storage.jdbc.util.IdColumn;
import io.spine.server.storage.jdbc.util.Serializer;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;

/**
 * A query for {@code INSERT}-ing multiple {@linkplain EntityRecord entity records} as a bulk.
 *
 * @author Dmytro Dashenkov
 */
public class InsertEntityRecordsBulkQuery<I> extends WriteQuery {

    private static final String FORMAT_PLACEHOLDER = "%s";

    private static final int COLUMNS_COUNT = 4;

    private static final String SQL_TEMPLATE = Sql.Query.INSERT_INTO + FORMAT_PLACEHOLDER +
                                               Sql.BuildingBlock.BRACKET_OPEN + RecordTable.Column.id + Sql.BuildingBlock.COMMA + RecordTable.Column.entity + Sql.BuildingBlock.COMMA +
                                               archived + Sql.BuildingBlock.COMMA + deleted + Sql.BuildingBlock.BRACKET_CLOSE +
                                               Sql.Query.VALUES + FORMAT_PLACEHOLDER;
    private static final String SQL_VALUES_TEMPLATE = Sql.nPlaceholders(COLUMNS_COUNT);

    private final Map<I, EntityRecord> records;
    private final IdColumn<I> idColumn;

    protected InsertEntityRecordsBulkQuery(Builder<I> builder) {
        super(builder);
        this.records = builder.records;
        this.idColumn = builder.idColumn;
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        int parameterCounter = 1;
        for (Map.Entry<I, EntityRecord> record : records.entrySet()) {
            final I id = record.getKey();
            final EntityRecord storageRecord = record.getValue();
            addRecordsParams(statement, parameterCounter, id, storageRecord);
            parameterCounter += COLUMNS_COUNT;
        }
        return statement;
    }

    /**
     * Adds an {@link EntityRecord} to the query.
     *
     * <p>This includes following items:
     * <ul>
     *     <li>ID;
     *     <li>serialized record by itself;
     *     <li>{@code Boolean} columns for the record visibility (archived and deleted fields).
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
                                  EntityRecord record) {
        int paramIndex = firstParamIndex;
        try {
            idColumn.setId(paramIndex, id, statement);
            paramIndex++;
            final byte[] bytes = Serializer.serialize(record);
            statement.setBytes(paramIndex, bytes);
            paramIndex++;
            final LifecycleFlags status = record.getLifecycleFlags();
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
            extends WriteQuery.Builder<Builder<I>, InsertEntityRecordsBulkQuery> {

        private Map<I, EntityRecord> records;
        private String tableName;
        private IdColumn<I> idColumn;

        public Builder<I> setRecords(Map<I, EntityRecord> records) {
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
            checkState(!isNullOrEmpty(tableName), "Table name is not set.");
            checkState(records != null, "Records field is not set.");

            final Collection<String> sqlValues = nCopies(records.size(),
                                                         SQL_VALUES_TEMPLATE);
            final String sqlValuesJoined = Joiner.on(Sql.BuildingBlock.COMMA.toString())
                                                 .join(sqlValues);
            final String sql = format(SQL_TEMPLATE, tableName, sqlValuesJoined) + Sql.BuildingBlock.SEMICOLON;
            setQuery(sql);
            return new InsertEntityRecordsBulkQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
