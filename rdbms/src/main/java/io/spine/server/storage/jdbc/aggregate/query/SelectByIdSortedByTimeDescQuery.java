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

package io.spine.server.storage.jdbc.aggregate.query;

import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.StorageQuery;
import io.spine.server.storage.jdbc.table.entity.aggregate.AggregateEventRecordTable;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.DbIterator;
import io.spine.server.storage.jdbc.util.IdColumn;
import io.spine.server.storage.jdbc.util.MessageDbIterator;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.DESC;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.ORDER_BY;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static io.spine.server.storage.jdbc.table.entity.aggregate.AggregateEventRecordTable.Column.aggregate;
import static io.spine.type.TypeUrl.of;
import static java.lang.String.format;

/**
 * Query that selects {@link AggregateEventRecord} by corresponding aggregate ID sorted by
 * time descending.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectByIdSortedByTimeDescQuery<I> extends StorageQuery {

    private static final String QUERY_TEMPLATE =
            SELECT.toString() + aggregate + FROM + "%s" +
            WHERE + AggregateEventRecordTable.Column.id + EQUAL + PLACEHOLDER +
            ORDER_BY + AggregateEventRecordTable.Column.timestamp + DESC + COMMA + AggregateEventRecordTable.Column.timestamp_nanos + DESC + SEMICOLON;

    private final IdColumn<I> idColumn;
    private final I id;

    private SelectByIdSortedByTimeDescQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    public DbIterator<AggregateEventRecord> execute() throws DatabaseException {
        try (ConnectionWrapper connection = getConnection(true);
             PreparedStatement statement = prepareStatement(connection)) {
            idColumn.setId(1, id, statement);
            return new MessageDbIterator<>(statement,
                                           aggregate.toString(),
                                           of(AggregateEventRecord.class));
        } catch (SQLException e) {
            getLogger().error("Error while selecting entity by aggregate id sorted by time: ", e);
            throw new DatabaseException(e);
        }
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I>
            extends StorageQuery.Builder<Builder<I>, SelectByIdSortedByTimeDescQuery> {

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
