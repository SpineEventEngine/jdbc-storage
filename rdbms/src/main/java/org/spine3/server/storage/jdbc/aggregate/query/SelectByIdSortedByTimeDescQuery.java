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

package org.spine3.server.storage.jdbc.aggregate.query;


import org.spine3.server.aggregate.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.DESC;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.ORDER_BY;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.AggregateRecord.AGGREGATE_COL;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.AggregateRecord.ID_COL;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.AggregateRecord.NANOS_COL;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.AggregateRecord.SECONDS_COL;

/**
 * Query that selects {@link AggregateStorageRecord} by corresponding aggregate ID sorted by time descending.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectByIdSortedByTimeDescQuery<I> extends StorageQuery {

    private final IdColumn<I> idColumn;
    private final I id;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            SELECT + AGGREGATE_COL + FROM + "%s" +
                    WHERE + ID_COL + EQUAL + PLACEHOLDER +
                    ORDER_BY + SECONDS_COL + DESC + COMMA + NANOS_COL + DESC + SEMICOLON;


    private SelectByIdSortedByTimeDescQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    public Iterator<AggregateStorageRecord> execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.getConnection(true);
             PreparedStatement statement = prepareStatement(connection)) {
            idColumn.setId(1, id, statement);
            return new DbIterator<>(statement, AGGREGATE_COL, AggregateStorageRecord.getDescriptor());
        } catch (SQLException e) {
            this.getLogger().error("Error while selecting entity by aggregate id sorted by time: ", e);
            throw new DatabaseException(e);
        }
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends StorageQuery.Builder<Builder<I>, SelectByIdSortedByTimeDescQuery> {

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
