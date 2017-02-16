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

package org.spine3.server.storage.jdbc.query;

import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.Query.DELETE_FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;

/**
 * A query for deleting one or many items by a value of a given column.
 *
 * @author Dmytro Dashenkov.
 */
public class DeleteRowQuery<I> extends StorageQuery {

    private static final String TEMPLATE =
            DELETE_FROM + "%s" + WHERE + "%s" + EQUAL + "%s";

    protected DeleteRowQuery(Builder<I> builder) {
        super(builder);
    }

    /**
     * Executes the {@code DELETE} SQL statement.
     *
     * @return {@code true} if at least one row was deleted, {@code false} otherwise
     */
    private boolean execute() {
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

    public static class Builder<V> extends StorageQuery.Builder<Builder<V>, DeleteRowQuery>{

        private String column;
        private V value;
        private String table;

        public Builder<V> setArgumgnt(String column, V value) {
            this.column = checkNotNull(column);
            this.value = checkNotNull(value);
            return getThis();
        }

        public Builder<V> setTable(String table) {
            this.table = checkNotNull(table);
            return getThis();
        }

        private String composeSql() {
            return format(TEMPLATE, table, column, idToString(value));
        }

        @Override
        public DeleteRowQuery<V> build() {
            checkNotNull(column, "ID column must be set first");
            checkNotNull(value, "ID value must be set first");
            checkNotNull(table, "Table must be set first");
            final String sql = composeSql();
            setQuery(sql);
            return new DeleteRowQuery<>(this);
        }

        @Override
        protected Builder<V> getThis() {
            return this;
        }
    }
}
