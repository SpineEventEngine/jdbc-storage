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
import io.spine.server.storage.jdbc.EventCountTable.Column;

import static io.spine.server.storage.jdbc.EventCountTable.Column.event_count;
import static io.spine.server.storage.jdbc.EventCountTable.Column.id;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SET;
import static io.spine.server.storage.jdbc.Sql.Query.UPDATE;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static java.lang.String.format;

/**
 * Query that updates event count in
 * the {@link EventCountTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class UpdateEventCountQuery<I> extends UpdateRecordQuery<I> {

    private final int count;

    private static final String QUERY_TEMPLATE =
            UPDATE + "%s" +
            SET + event_count + EQUAL + PLACEHOLDER +
            WHERE + id + EQUAL + PLACEHOLDER + SEMICOLON;

    private UpdateEventCountQuery(Builder<I> builder) {
        super(builder);
        this.count = builder.count;
    }

    @Override
    protected Parameters getQueryParameters() {
        final Parameters superParameters = super.getQueryParameters();
        final Parameter countParameter = Parameter.of(count, Column.event_count);
        return Parameters.newBuilder()
                         .addParameters(superParameters)
                         .addParameter(1, countParameter)
                         .build();
    }

    static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(QUERY_TEMPLATE, tableName))
               .setIdIndexInQuery(2);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends UpdateRecordQuery.Builder<Builder<I>,
                                                              UpdateEventCountQuery, I> {

        private int count;

        @Override
        public UpdateEventCountQuery<I> build() {
            return new UpdateEventCountQuery<>(this);
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
