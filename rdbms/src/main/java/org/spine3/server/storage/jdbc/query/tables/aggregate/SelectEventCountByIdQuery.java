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

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;

public class SelectEventCountByIdQuery<Id> extends AbstractQuery {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_QUERY =
            "SELECT " + AggregateTable.EVENT_COUNT_COL +
                    " FROM %s " +
                    " WHERE " + AggregateTable.ID_COL + " = ?;";

    private final IdColumn<Id> idColumn;
    private final Id id;

    private SelectEventCountByIdQuery(Builder<Id> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    @Nullable
    public Integer execute() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = prepareStatement(connection);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final int eventCount = resultSet.getInt(AggregateTable.EVENT_COUNT_COL);
            return eventCount;
        } catch (SQLException e) {
            // log().error("Failed to read an event count after the last snapshot.", e);
            throw new DatabaseException(e);
        }
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        idColumn.setId(1, id, statement);
        return statement;
    }

    public static <Id> Builder<Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<Id>();
        builder.setQuery(format(SELECT_QUERY, tableName));
        return builder;
    }

    public static class Builder<Id> extends AbstractQuery.Builder<Builder<Id>, SelectEventCountByIdQuery> {

        private IdColumn<Id> idColumn;
        private Id id;

        @Override
        public SelectEventCountByIdQuery<Id> build() {
            return new SelectEventCountByIdQuery<>(this);
        }

        public Builder<Id> setIdColumn(IdColumn<Id> idColumn){
            this.idColumn = idColumn;
            return getThis();
        }

        public Builder<Id> setId(Id id){
            this.id = id;
            return getThis();
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
