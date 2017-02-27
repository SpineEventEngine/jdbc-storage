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

package org.spine3.server.storage.jdbc.entity.query;

import org.spine3.server.entity.EntityRecord;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;

import static java.lang.String.format;
import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.Sql.nPlaceholders;
import static org.spine3.server.storage.jdbc.entity.query.EntityTable.ENTITY_COL;
import static org.spine3.server.storage.jdbc.entity.query.EntityTable.ID_COL;

/**
 * Query that inserts a new {@link EntityRecord} to the {@link EntityTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class InsertEntityQuery<I> extends WriteEntityQuery<I> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            INSERT_INTO + " %s " +
            BRACKET_OPEN +
            ENTITY_COL + COMMA + archived + COMMA + deleted + COMMA + ID_COL +
            BRACKET_CLOSE +
            VALUES + nPlaceholders(4) + SEMICOLON;

    private InsertEntityQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setIdIndexInQuery(QueryParameter.ID.index)
               .setRecordIndexInQuery(QueryParameter.RECORD.index)
               .setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends WriteRecordQuery.Builder<Builder<I>, InsertEntityQuery, I, EntityRecord> {

        @Override
        public InsertEntityQuery build() {
            return new InsertEntityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
