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

import io.spine.Identifier;
import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.Query.DELETE_FROM;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static java.lang.String.format;

/**
 * A query for deleting one or many items by a id of a given column.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public class DeleteRecordQuery<I> extends StorageQuery {

    private static final String FORMAT_PLACEHOLDER = "%s";

    private static final String TEMPLATE =
            DELETE_FROM + FORMAT_PLACEHOLDER + WHERE + FORMAT_PLACEHOLDER + EQUAL + PLACEHOLDER;

    private static final int COLUMN_VALUE_PARAM_INDEX = 1;

    private final I id;
    private final IdColumn<I> idColumn;

    protected DeleteRecordQuery(Builder<I> builder) {
        super(builder);
        this.id = builder.columnValue;
        this.idColumn = builder.idColumn;
    }

    /**
     * Executes the {@code DELETE} SQL statement.
     *
     * @return {@code true} if at least one row was deleted, {@code false} otherwise
     */
    public boolean execute() {
        try (ConnectionWrapper connection = getConnection(false)) {
            final PreparedStatement statement = prepareStatement(connection);
            final int rowsAffected = statement.executeUpdate();
            connection.commit();
            final boolean result = rowsAffected != 0;
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected Parameters getQueryParameters() {
        final Parameters.Builder builder = Parameters.newBuilder();
        idColumn.setId(COLUMN_VALUE_PARAM_INDEX, id, builder);
        return builder.build();
    }

    public static <V> Builder<V> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageQuery.Builder<Builder<I>, DeleteRecordQuery> {

        private I columnValue;
        private String table;
        private IdColumn<I> idColumn;

        public Builder<I> setIdValue(I value) {
            this.columnValue = checkNotNull(value);
            return getThis();
        }

        public Builder<I> setTableName(String table) {
            this.table = checkNotNull(table);
            return getThis();
        }

        private String composeSql() {
            return format(TEMPLATE,
                          table,
                          idColumn.getColumnName(),
                          Identifier.toString(columnValue));
        }

        @Override
        public DeleteRecordQuery<I> build() {
            checkNotNull(idColumn, "ID column must be set");
            checkNotNull(columnValue, "ID must be set");
            checkNotNull(table, "Table must be set");
            final String sql = composeSql();
            setQuery(sql);
            return new DeleteRecordQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
