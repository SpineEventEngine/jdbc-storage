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

package org.spine3.server.storage.jdbc.aggregate.query;


import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;
import static org.spine3.server.storage.jdbc.aggregate.query.Constants.EVENT_COUNT_COL;
import static org.spine3.server.storage.jdbc.aggregate.query.Constants.ID_COL;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;

public class SelectEventCountByIdQuery<I> extends AbstractQuery {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_QUERY =
            "SELECT " + EVENT_COUNT_COL +
                    " FROM %s " +
                    " WHERE " + ID_COL + " = ?;";

    private final IdColumn<I> idColumn;
    private final I id;

    private SelectEventCountByIdQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    @Nullable
    public Integer execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.getDataSource().getConnection(true);
             PreparedStatement statement = prepareStatement(connection);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final int eventCount = resultSet.getInt(EVENT_COUNT_COL);
            return eventCount;
        } catch (SQLException e) {
            this.getLogger().error("Failed to read an event count after the last snapshot.", e);
            throw new DatabaseException(e);
        }
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        idColumn.setId(1, id, statement);
        return statement;
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(SELECT_QUERY, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends AbstractQuery.Builder<Builder<I>, SelectEventCountByIdQuery> {

        private IdColumn<I> idColumn;
        private I id;

        @Override
        public SelectEventCountByIdQuery<I> build() {
            return new SelectEventCountByIdQuery<>(this);
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn){
            this.idColumn = idColumn;
            return getThis();
        }

        public Builder<I> setId(I id){
            this.id = id;
            return getThis();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
