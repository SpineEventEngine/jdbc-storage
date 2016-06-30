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

package org.spine3.server.storage.jdbc.query.tables.event;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.EventTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;

public class CreateTableIfDoesNotExistQuery extends AbstractQuery {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE IF NOT EXISTS " + EventTable.TABLE_NAME + " (" +
                    EventTable.EVENT_ID_COL + " VARCHAR(512), " +
                    EventTable.EVENT_COL + " BLOB, " +
                    EventTable.EVENT_TYPE_COL + " VARCHAR(512), " +
                    EventTable.PRODUCER_ID_COL + " VARCHAR(512), " +
                    EventTable.SECONDS_COL + " BIGINT, " +
                    EventTable.NANOSECONDS_COL + " INT, " +
                    " PRIMARY KEY(" + EventTable.EVENT_ID_COL + ')' +
                    ");";

    protected CreateTableIfDoesNotExistQuery(Builder builder) {
        super(builder);
    }

    public void execute() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = prepareStatement(connection)) {
            statement.execute();
        } catch (SQLException e) {
            //log().error("Error while creating a table with the name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(CREATE_TABLE_QUERY);
        return builder;
    }

    public static class Builder extends AbstractQuery.Builder<Builder, CreateTableIfDoesNotExistQuery> {

        @Override
        public CreateTableIfDoesNotExistQuery build() {
            return new CreateTableIfDoesNotExistQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}