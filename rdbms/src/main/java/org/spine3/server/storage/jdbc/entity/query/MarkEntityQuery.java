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

import org.spine3.server.storage.EntityStatusField;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.entity.status.query.UpdateEntityStatusQuery;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SET;
import static org.spine3.server.storage.jdbc.Sql.Query.TRUE;
import static org.spine3.server.storage.jdbc.Sql.Query.UPDATE;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.ID_COL;

/**
 * @author Dmytro Dashenkov.
 */
public class MarkEntityQuery extends StorageQuery {

    private static final String SQL_TEMPLATE = UPDATE + "%s" + SET +
            "%s" + EQUAL + TRUE +
            WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    private final String id;

    protected MarkEntityQuery(Builder builder) {
        super(builder);
        this.id = builder.id;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        try {
            statement.setString(1, id);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return statement;
    }

    public boolean execute() {
        try (PreparedStatement statement = prepareStatement(getConnection(false))) {
            final int rowsAffected = statement.executeUpdate();
            checkState(rowsAffected <= 1,
                       "Mark query affected more then one row: " + getQuery());
            return rowsAffected > 0;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public static UpdateEntityStatusQuery.Builder newBuilder() {
        final UpdateEntityStatusQuery.Builder builder = new UpdateEntityStatusQuery.Builder();
        return builder;
    }

    public static class Builder extends StorageQuery.Builder<MarkEntityQuery.Builder, MarkEntityQuery> {

        private String id;
        private String column;
        private String tableName;

        public Builder setId(Object id) {
            checkNotNull(id);
            this.id = idToString(id);
            return getThis();
        }

        public Builder setColumn(EntityStatusField column) {
            checkNotNull(column);
            this.column = column.toString();
            return getThis();
        }

        public Builder setTableName(String tableName) {
            checkNotNull(tableName);
            checkArgument(!tableName.isEmpty());
            this.tableName = tableName;
            return getThis();
        }

        @Override
        public MarkEntityQuery build() {
            checkState(id != null, "Record ID is not set.");
            checkState(!isNullOrEmpty(column), "Column to mark is not set.");
            checkState(!isNullOrEmpty(tableName), "Table is not set.");

            final String sql = String.format(SQL_TEMPLATE, tableName, column);
            setQuery(sql);
            return new MarkEntityQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
