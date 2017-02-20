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

import org.spine3.server.entity.status.EntityStatus;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SET;
import static org.spine3.server.storage.jdbc.Sql.Query.UPDATE;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.ARCHIVED_COL;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.DELETED_COL;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.ID_COL;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.TABLE_NAME;

/**
 * @author Dmytro Dashenkov.
 */
public class UpdateEntityStatusQuery extends WriteQuery {

    private static final String SQL = UPDATE + TABLE_NAME + SET +
            ARCHIVED_COL + EQUAL + PLACEHOLDER + COMMA +
            DELETED_COL + EQUAL + PLACEHOLDER +
            WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    private final String id;
    private final EntityStatus entityStatus;

    protected UpdateEntityStatusQuery(Builder builder) {
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
            statement.setBoolean(1, archived);
            statement.setBoolean(2, deleted);
            statement.setString(3, id);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return statement;
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(SQL);
        return builder;
    }

    public static class Builder extends WriteQuery.Builder<Builder, UpdateEntityStatusQuery> {

        private String id;
        private EntityStatus entityStatus;

        public Builder setId(Object id) {
            checkNotNull(id);
            final String stringId = idToString(id);
            this.id = stringId;
            return getThis();
        }

        public Builder setEntityStatus(EntityStatus status) {
            this.entityStatus = checkNotNull(status);
            return getThis();
        }

        @Override
        public UpdateEntityStatusQuery build() {
            checkState(id != null, "ID is not set.");
            checkState(entityStatus != null, "Entity status is not set.");
            return new UpdateEntityStatusQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
