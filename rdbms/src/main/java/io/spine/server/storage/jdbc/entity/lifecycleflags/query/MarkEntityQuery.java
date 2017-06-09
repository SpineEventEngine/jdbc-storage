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

package io.spine.server.storage.jdbc.entity.lifecycleflags.query;

import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.query.StorageQuery;
import io.spine.server.storage.jdbc.table.entity.aggregate.LifecycleFlagsTable;
import io.spine.server.storage.LifecycleFlagField;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;

/**
 * The query to the {@linkplain io.spine.server.entity.LifecycleFlags entity lifecycle flags}
 * setting one of the columns {@code true}.
 *
 * @author Dmytro Dashenkov
 */
public class MarkEntityQuery<I> extends StorageQuery {

    private static final String FORMAT_PLACEHOLDER = "%s";

    private static final int ID_PARAM_INDEX = 1;

    private static final String SQL_TEMPLATE = Sql.Query.UPDATE + FORMAT_PLACEHOLDER + Sql.Query.SET +
                                               FORMAT_PLACEHOLDER + Sql.BuildingBlock.EQUAL + Sql.Query.TRUE +
                                               Sql.Query.WHERE + LifecycleFlagsTable.Column.id + Sql.BuildingBlock.EQUAL + Sql.Query.PLACEHOLDER + Sql.BuildingBlock.SEMICOLON;

    private final I id;
    private final IdColumn<I> idColumn;

    protected MarkEntityQuery(AbstractMarkQueryBuilder<I, ?, ?> builder) {
        super(builder);
        this.id = builder.getId();
        this.idColumn = builder.getIdColumn();
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        idColumn.setId(ID_PARAM_INDEX, id, statement);

        return statement;
    }

    public boolean execute() {
        try (PreparedStatement statement = prepareStatement(getConnection(true))) {
            final int rowsAffected = statement.executeUpdate();
            checkState(rowsAffected <= 1,
                       "Mark query affected more then one row: " + getQuery());
            return rowsAffected > 0;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public static <I> MarkEntityQuery.Builder<I> newBuilder() {
        final Builder<I> builder = new Builder<>();
        return builder;
    }

    protected abstract static class AbstractMarkQueryBuilder<
            I,
            B extends AbstractMarkQueryBuilder<I, B, Q>,
            Q extends StorageQuery>
            extends StorageQuery.Builder<B, Q> {

        private I id;
        private String column;
        private String tableName;
        private IdColumn<I> idColumn;

        protected abstract Q newQuery();

        protected abstract String buildSql();

        public B setId(I id) {
            checkNotNull(id);
            this.id = id;
            return getThis();
        }

        public B setColumn(LifecycleFlagField column) {
            checkNotNull(column);
            this.column = column.toString();
            return getThis();
        }

        public B setTableName(String tableName) {
            this.tableName = checkNotNull(tableName);
            return getThis();
        }

        public B setIdColumn(IdColumn<I> idColumn) {
            checkNotNull(idColumn);
            this.idColumn = idColumn;
            return getThis();
        }

        public I getId() {
            return id;
        }

        public String getColumn() {
            return column;
        }

        public String getTableName() {
            return tableName;
        }

        public IdColumn<I> getIdColumn() {
            return idColumn;
        }

        @Override
        public Q build() {
            checkState(id != null, "Record ID is not set.");
            checkState(!isNullOrEmpty(column), "Column to mark is not set.");
            checkState(!isNullOrEmpty(tableName), "Table is not set.");

            final String sql = buildSql();
            setQuery(sql);
            return newQuery();
        }
    }

    public static class Builder<I>
            extends AbstractMarkQueryBuilder<I, Builder<I>, MarkEntityQuery<I>> {

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        @Override
        protected MarkEntityQuery<I> newQuery() {
            return new MarkEntityQuery<>(this);
        }

        @Override
        protected String buildSql() {
            return format(SQL_TEMPLATE, getTableName(), getColumn());
        }
    }
}
