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

package org.spine3.server.storage.jdbc.projection.query;

import org.spine3.server.storage.jdbc.query.CreateTableQuery;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static org.spine3.server.storage.jdbc.Sql.Type.BIGINT;
import static org.spine3.server.storage.jdbc.Sql.Type.INT;
import static org.spine3.server.storage.jdbc.projection.query.ProjectionTable.NANOS_COL;
import static org.spine3.server.storage.jdbc.projection.query.ProjectionTable.SECONDS_COL;

/**
 * Query that creates a new {@link ProjectionTable} if it does not exist.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class CreateProjectionTableQuery extends CreateTableQuery<String> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            CREATE_IF_MISSING + "%s " + BRACKET_OPEN +
                    SECONDS_COL + BIGINT + COMMA +
                    NANOS_COL + INT + COMMA +
                    BRACKET_CLOSE + SEMICOLON;

    protected CreateProjectionTableQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends CreateTableQuery.Builder<Builder, CreateProjectionTableQuery, String> {

        @Override
        public CreateProjectionTableQuery build() {
            return new CreateProjectionTableQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
