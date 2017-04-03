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
import org.spine3.server.storage.jdbc.table.CommandTable;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.Sql.nPlaceholders;
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.command;
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.command_status;
import static org.spine3.server.storage.jdbc.table.CommandTable.Column.id;
import static org.spine3.server.storage.jdbc.table.TableColumns.getIndex;

/**
 * Query that inserts a new {@link CommandRecord} to
 * the {@link org.spine3.server.storage.jdbc.table.CommandTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class InsertCommandQuery extends WriteCommandRecordQuery {

    private static final String QUERY_TEMPLATE =
            INSERT_INTO + CommandTable.TABLE_NAME + BRACKET_OPEN +
            id + COMMA +
            command + COMMA +
            command_status + BRACKET_CLOSE +
            VALUES + nPlaceholders(3) + SEMICOLON;

    private InsertCommandQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setStatusIndexInQuery(getIndex(command_status))
               .setIdIndexInQuery(getIndex(id))
               .setRecordIndexInQuery(getIndex(command))
               .setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder
            extends WriteCommandRecordQuery.Builder<Builder, InsertCommandQuery> {

        @Override
        public InsertCommandQuery build() {
            return new InsertCommandQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
