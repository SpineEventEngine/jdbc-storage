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

package org.spine3.server.storage.jdbc.command.query;

import org.spine3.server.command.CommandRecord;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SET;
import static org.spine3.server.storage.jdbc.Sql.Query.UPDATE;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.COMMAND_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.COMMAND_STATUS_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.ID_COL;
import static org.spine3.server.storage.jdbc.command.query.CommandTable.TABLE_NAME;

/**
 * Query that updates {@link CommandRecord} in the {@link CommandTable}.
 *
 * @author Andrey Lavrov
 */
public class UpdateCommandQuery extends WriteCommandRecordQuery {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            UPDATE + TABLE_NAME +
            SET + COMMAND_COL + EQUAL + PLACEHOLDER +
            COMMA + COMMAND_STATUS_COL + EQUAL + PLACEHOLDER +
            WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    private UpdateCommandQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setStatusIndexInQuery(QueryParameters.STATUS.index)
               .setIdIndexInQuery(QueryParameters.ID.index)
               .setRecordIndexInQuery(QueryParameters.RECORD.index)
               .setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends WriteCommandRecordQuery.Builder<Builder, UpdateCommandQuery> {

        @Override
        public UpdateCommandQuery build() {
            return new UpdateCommandQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }

    private enum QueryParameters {

        RECORD(1),
        STATUS(2),
        ID(3);

        private final int index;

        QueryParameters(int index) {
            this.index = index;
        }
    }
}
