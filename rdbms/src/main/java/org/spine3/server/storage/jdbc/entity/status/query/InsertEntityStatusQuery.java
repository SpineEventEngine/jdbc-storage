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

import org.spine3.base.Stringifiers;
import org.spine3.server.entity.status.EntityStatus;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.Sql.nPlaceholders;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.COLUMN_COUNT;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.TABLE_NAME;

/**
 * @author Dmytro Dashenkov.
 */
public class InsertEntityStatusQuery extends WriteQuery {

    private static final String SQL = INSERT_INTO + TABLE_NAME +
            VALUES + BRACKET_OPEN + nPlaceholders(COLUMN_COUNT) + BRACKET_CLOSE + SEMICOLON;

    private final String id;
    private final EntityStatus entityStatus;

    protected InsertEntityStatusQuery(Builder builder) {
        super(builder);
        this.id = builder.id;
        this.entityStatus = builder.entityStatus;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        final boolean archived = entityStatus.getArchived();
        final boolean deleted = entityStatus.getDeleted();
        try {
            statement.setString(1, id);
            statement.setBoolean(2, archived);
            statement.setBoolean(3, deleted);
        } catch (SQLException e) {
            logWriteError(id, e);
            throw new DatabaseException(e);
        }
        return statement;
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(SQL);
        return builder;
    }

    public static class Builder extends WriteQuery.Builder<Builder, InsertEntityStatusQuery> {

        private String id;
        private EntityStatus entityStatus;

        public Builder setEntityStatus(EntityStatus status) {
            this.entityStatus = checkNotNull(status);
            return getThis();
        }

        public Builder setId(Object id) {
            checkNotNull(id);
            final String stringId = Stringifiers.idToString(id);
            this.id = stringId;
            return getThis();
        }

        @Override
        public InsertEntityStatusQuery build() {
            checkNotNull(id, "ID is not set.");
            checkNotNull(entityStatus, "Entity status is not set.");
            return new InsertEntityStatusQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
