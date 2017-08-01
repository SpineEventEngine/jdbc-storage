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

import com.google.protobuf.Int32Value;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static io.spine.server.storage.jdbc.EventCountTable.Column.event_count;
import static io.spine.server.storage.jdbc.EventCountTable.Column.id;
import static java.lang.String.format;

/**
 * Query that selects event count by corresponding aggregate ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class SelectEventCountByIdQuery<I> extends SelectMessageByIdQuery<I, Int32Value> {

    private static final String QUERY_TEMPLATE =
            SELECT.toString() + event_count +
            FROM + "%s" +
            WHERE + id + EQUAL + PLACEHOLDER + SEMICOLON;

    private SelectEventCountByIdQuery(Builder<I> builder) {
        super(builder);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Override default message storing policy
    @Override
    protected Int32Value readMessage(ResultSet resultSet) throws SQLException {
        final int eventCount = resultSet.getInt(event_count.name());
        return Int32Value.newBuilder()
                         .setValue(eventCount)
                         .build();
    }

    static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(QUERY_TEMPLATE, tableName))
               .setIdIndexInQuery(1);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends SelectMessageByIdQuery.Builder<Builder<I>,
                                                                   SelectEventCountByIdQuery<I>,
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
