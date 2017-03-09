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

import com.google.protobuf.Int32Value;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.EventCount.EVENT_COUNT_COL;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.EventCount.ID_COL;

/**
 * Query that selects event count by corresponding aggregate ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectEventCountByIdQuery<I> extends SelectByIdQuery<I, Int32Value> {

    private static final String QUERY_TEMPLATE =
            SELECT + EVENT_COUNT_COL +
            FROM + "%s" +
            WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    private SelectEventCountByIdQuery(Builder<I> builder) {
        super(builder);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Override default message storing policy
    @Override
    protected Int32Value readMessage(ResultSet resultSet) throws SQLException {
        final int eventCount = resultSet.getInt(EVENT_COUNT_COL);
        return Int32Value.newBuilder()
                         .setValue(eventCount)
                         .build();
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(QUERY_TEMPLATE, tableName))
               .setIdIndexInQuery(1);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends SelectByIdQuery.Builder<Builder<I>,
                                                                   SelectEventCountByIdQuery,
                                                                   I,
                                                                   Int32Value> {
        @Override
        public SelectEventCountByIdQuery<I> build() {
            return new SelectEventCountByIdQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
