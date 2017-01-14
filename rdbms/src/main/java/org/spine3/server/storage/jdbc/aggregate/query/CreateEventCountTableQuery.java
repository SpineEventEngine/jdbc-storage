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

import org.spine3.server.storage.jdbc.query.CreateTableQuery;

import static org.spine3.server.storage.jdbc.Sql.Common.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.Common.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.Common.COMMA;
import static org.spine3.server.storage.jdbc.Sql.Common.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static org.spine3.server.storage.jdbc.Sql.Type.BIGINT;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.EventCount.EVENT_COUNT_COL;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.EventCount.ID_COL;

/**
 * Query that creates a new {@link Table.EventCount} if it does not exist.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class CreateEventCountTableQuery<I> extends CreateTableQuery<I> {

    private static final String FORMAT_PLACEHOLDER = " %s ";

    private static final String QUERY_TEMPLATE =
                    CREATE_IF_MISSING + FORMAT_PLACEHOLDER + BRACKET_OPEN +
                    ID_COL + FORMAT_PLACEHOLDER + COMMA +
                    EVENT_COUNT_COL + BIGINT + BRACKET_CLOSE + SEMICOLON;

    protected CreateEventCountTableQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I> Builder<I> newBuilder() {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends CreateTableQuery.Builder<Builder<I>, CreateEventCountTableQuery, I> {

        @Override
        public CreateEventCountTableQuery build() {
            return new CreateEventCountTableQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
