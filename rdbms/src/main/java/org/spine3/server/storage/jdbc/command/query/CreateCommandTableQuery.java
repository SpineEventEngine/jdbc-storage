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

package org.spine3.server.storage.jdbc.command.query;

import org.spine3.server.storage.jdbc.query.CreateTableQuery;

import static org.spine3.server.storage.jdbc.command.query.CommandTable.*;

/**
 * Query that creates a new {@link CommandTable} if it does not exist.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class CreateCommandTableQuery extends CreateTableQuery<String> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    ID_COL + " %s, " +
                    COMMAND_COL + " BLOB, " +
                    COMMAND_STATUS_COL + " VARCHAR(512), " +
                    ERROR_COL + " BLOB, " +
                    FAILURE_COL + " BLOB, " +
                    " PRIMARY KEY(" + ID_COL + ')' +
                    ");";

    public CreateCommandTableQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(INSERT_QUERY);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends CreateTableQuery.Builder<Builder, CreateCommandTableQuery, String> {

        @Override
        public CreateCommandTableQuery build() {
            return new CreateCommandTableQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
