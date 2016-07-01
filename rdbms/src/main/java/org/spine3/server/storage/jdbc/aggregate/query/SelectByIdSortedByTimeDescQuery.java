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


import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;
import org.spine3.server.storage.jdbc.util.IdColumn;
import static org.spine3.server.storage.jdbc.aggregate.query.Constants.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static java.lang.String.format;

public class SelectByIdSortedByTimeDescQuery<Id> extends AbstractQuery {

    private final IdColumn<Id> idColumn;
    private final Id id;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_BY_ID_SORTED_BY_TIME_DESC =
            "SELECT " + AGGREGATE_COL + " FROM %s " +
                    " WHERE " + ID_COL + " = ? " +
                    " ORDER BY " + SECONDS_COL + " DESC, " + NANOS_COL + " DESC;";


    private SelectByIdSortedByTimeDescQuery(Builder<Id> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    public Iterator<AggregateStorageRecord> execute() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = prepareStatement(connection)) {
            idColumn.setId(1, id, statement);
            return new DbIterator<>(statement, AGGREGATE_COL, RECORD_DESCRIPTOR);
        } catch (SQLException e) {
            //log().error("Error while creating a table with the name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    public static <Id> Builder<Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<>();
        builder.setQuery(format(SELECT_BY_ID_SORTED_BY_TIME_DESC, tableName));
        return builder;
    }

    public static class Builder<I> extends AbstractQuery.Builder<Builder<I>, SelectByIdSortedByTimeDescQuery> {

        private IdColumn<I> idColumn;
        private I id;

        @Override
        public SelectByIdSortedByTimeDescQuery<I> build() {
            return new SelectByIdSortedByTimeDescQuery<>(this);
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        public Builder<I> setId(I id) {
            this.id = id;
            return getThis();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
