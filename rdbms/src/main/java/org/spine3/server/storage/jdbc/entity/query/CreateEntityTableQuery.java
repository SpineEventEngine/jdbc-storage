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

package org.spine3.server.storage.jdbc.entity.query;

import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.query.CreateTableQuery;

import static org.spine3.server.storage.jdbc.Sql.Common.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.Common.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.Common.COMMA;
import static org.spine3.server.storage.jdbc.Sql.Common.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static org.spine3.server.storage.jdbc.Sql.Query.PRIMARY_KEY;
import static org.spine3.server.storage.jdbc.entity.query.EntityTable.ENTITY_COL;
import static org.spine3.server.storage.jdbc.entity.query.EntityTable.ID_COL;

/**
 * Query that creates a new {@link EntityTable} if it does not exist.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class CreateEntityTableQuery<I> extends CreateTableQuery<I> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            CREATE_IF_MISSING + " %s " + BRACKET_OPEN +
                    ID_COL + " %s, " +
                    ENTITY_COL + Sql.Type.BLOB + COMMA +
                    PRIMARY_KEY + ID_COL + BRACKET_CLOSE +
                    BRACKET_CLOSE + SEMICOLON;

    protected CreateEntityTableQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I>Builder<I> newBuilder() {
        final Builder <I> builder = new Builder<>();
        builder.setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends CreateTableQuery.Builder<Builder<I>, CreateEntityTableQuery, I> {

        @Override
        public CreateEntityTableQuery build() {
            return new CreateEntityTableQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
