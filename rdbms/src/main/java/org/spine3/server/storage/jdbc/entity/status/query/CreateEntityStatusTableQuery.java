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

package org.spine3.server.storage.jdbc.entity.status.query;

import org.spine3.server.storage.jdbc.query.CreateTableQuery;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static org.spine3.server.storage.jdbc.Sql.Type.BOOLEAN;
import static org.spine3.server.storage.jdbc.Sql.Type.VARCHAR_512;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.ARCHIVED_COL;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.DELETED_COL;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.ID_COL;

/**
 * @author Dmytro Dashenkov.
 */
public class CreateEntityStatusTableQuery extends CreateTableQuery<String> {

    private static final String SQL_TEMPLATE = CREATE_IF_MISSING + "%s" + BRACKET_OPEN +
            ID_COL + VARCHAR_512 + COMMA +
            ARCHIVED_COL + BOOLEAN + COMMA +
            DELETED_COL + BOOLEAN + BRACKET_CLOSE + SEMICOLON;


    protected CreateEntityStatusTableQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(SQL_TEMPLATE);
        return builder;
    }

    public static class Builder extends CreateTableQuery.Builder<Builder, CreateEntityStatusTableQuery, String> {

        @SuppressWarnings("MethodDoesntCallSuperMethod")// We don't want to override the {@code idColumn} field.
        @Deprecated
        @Override
        public Builder setIdColumn(IdColumn<String> idColumn) {
            throw new UnsupportedOperationException("Entity Status table has predefined id type: String.");
        }

        @Override
        public CreateEntityStatusTableQuery build() {
            return new CreateEntityStatusTableQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}