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

package io.spine.server.storage.jdbc.query;

import io.spine.server.storage.jdbc.EventCountTable;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static io.spine.server.storage.jdbc.Sql.Query.VALUES;
import static io.spine.server.storage.jdbc.EventCountTable.Column.event_count;
import static io.spine.server.storage.jdbc.EventCountTable.Column.id;

/**
 * Query that inserts a new aggregate event count after the last snapshot to the
 * {@link EventCountTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class InsertEventCountQuery<I> extends UpdateRecordQuery<I> {

    private static final int ID_INDEX = id.ordinal() + 1;
    private static final int EVENT_COUNT_INDEX = event_count.ordinal() + 1;

    private static final String QUERY_TEMPLATE =
            INSERT_INTO + " %s " +
            BRACKET_OPEN + id + COMMA + event_count + BRACKET_CLOSE +
            VALUES + Sql.nPlaceholders(2) + SEMICOLON;

    private final int count;

    private InsertEventCountQuery(Builder<I> builder) {
        super(builder);
        this.count = builder.count;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        try {
            statement.setInt(EVENT_COUNT_INDEX, count);
            return statement;
        } catch (SQLException e) {
            logFailedToPrepareStatement(e);
            throw new DatabaseException(e);
        }
    }

    static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(QUERY_TEMPLATE, tableName))
               .setIdIndexInQuery(ID_INDEX);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I>
            extends UpdateRecordQuery.Builder<Builder<I>, InsertEventCountQuery, I> {

        private int count;

        @Override
        public InsertEventCountQuery<I> build() {
            return new InsertEventCountQuery<>(this);
        }

        Builder<I> setCount(int count) {
            this.count = count;
            return getThis();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}