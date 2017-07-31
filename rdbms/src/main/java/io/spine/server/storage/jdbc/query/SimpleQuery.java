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

import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A query for executing a generic SQL.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public class SimpleQuery extends StorageQuery {

    private SimpleQuery(Builder builder) {
        super(builder);
    }

    /**
     * Executes the given SQL query and ignores the result.
     */
    public void execute() {
        try (ConnectionWrapper connection = getConnection(true);
             PreparedStatement statement = prepareStatement(connection)) {
            statement.execute();
        } catch (SQLException e) {
            getLogger().error("Error executing statement " + getQuery(), e);
            throw new DatabaseException(e);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends StorageQuery.Builder<Builder, SimpleQuery> {

        private Builder() {
            super();
        }

        @Override
        public SimpleQuery build() {
            return new SimpleQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
