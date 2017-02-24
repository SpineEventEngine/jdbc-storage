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

package org.spine3.server.storage.jdbc.entity.query;

import org.spine3.server.storage.VisibilityField;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SET;
import static org.spine3.server.storage.jdbc.Sql.Query.TRUE;
import static org.spine3.server.storage.jdbc.Sql.Query.UPDATE;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable.ID_COL;

/**
 * @author Dmytro Dashenkov.
 */
public class MarkEntityQuery<I> extends StorageQuery {

    private static final String FORMAT_PLACEHOLDER = "%s";

    private static final String SQL_TEMPLATE = UPDATE + FORMAT_PLACEHOLDER + SET +
                                               FORMAT_PLACEHOLDER + EQUAL + TRUE +
                                               WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

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
        idColumn.setId(1, id, statement);

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

    protected abstract static class AbstractMarkQueryBuilder<I, B extends AbstractMarkQueryBuilder<I, B, Q>, Q extends StorageQuery> extends StorageQuery.Builder<B, Q> {

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

        public B setColumn(VisibilityField column) {
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

    public static class Builder<I> extends AbstractMarkQueryBuilder<I, Builder<I>, MarkEntityQuery<I>> {

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
            return String.format(SQL_TEMPLATE, getTableName(), getColumn());
        }
    }
}
