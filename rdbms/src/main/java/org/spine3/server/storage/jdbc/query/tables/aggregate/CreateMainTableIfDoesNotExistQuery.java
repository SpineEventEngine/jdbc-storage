/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.query.tables.aggregate;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.AggregateTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;

public class CreateMainTableIfDoesNotExistQuery<I> extends AbstractQuery {

    private final IdColumn<I> idColumn;
    private final String tableName;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    AggregateTable.ID_COL + " %s, " +
                    AggregateTable.AGGREGATE_COL + " BLOB, " +
                    AggregateTable.SECONDS_COL + " BIGINT, " +
                    AggregateTable.NANOS_COL + " INT " +
                    ");";

    protected CreateMainTableIfDoesNotExistQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.tableName = builder.tableName;
    }

    public void execute() throws DatabaseException {
        final String idColumnType = idColumn.getColumnDataType();
        final String createTableSql = format(QUERY, tableName, idColumnType);
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(createTableSql)) {
            statement.execute();
        } catch (SQLException e) {
            //log().error("Error while creating a table with the name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    public static <I>Builder <I>getBuilder() {
        final Builder <I>builder = new Builder<>();
        builder.setQuery(QUERY);
        return builder;
    }

    public static class Builder<I> extends AbstractQuery.Builder<Builder<I>, CreateMainTableIfDoesNotExistQuery> {

        private IdColumn<I> idColumn;
        private String tableName;

        @Override
        public CreateMainTableIfDoesNotExistQuery build() {
            return new CreateMainTableIfDoesNotExistQuery<I>(this);
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        public Builder<I> setTableName(String tableName) {
            this.tableName = tableName;
            return getThis();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}