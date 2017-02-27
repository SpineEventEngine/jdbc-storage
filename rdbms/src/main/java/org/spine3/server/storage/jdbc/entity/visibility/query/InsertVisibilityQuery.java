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

package org.spine3.server.storage.jdbc.entity.visibility.query;

import org.spine3.base.Stringifiers;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.Sql.nPlaceholders;
import static org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable.TABLE_NAME;

/**
 * The query for creating a new record in the table storing
 * the {@linkplain org.spine3.server.entity.Visibility entity visibility}.
 *
 * @author Dmytro Dashenkov.
 */
public class InsertVisibilityQuery extends WriteQuery {

    private static final int COLUMN_COUNT = 3;
    private static final String SQL = INSERT_INTO + TABLE_NAME +
                                      VALUES + nPlaceholders(COLUMN_COUNT) + SEMICOLON;

    private final String id;
    private final Visibility entityStatus;

    protected InsertVisibilityQuery(Builder builder) {
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
            statement.setString(TableColumn.ID.index, id);
            statement.setBoolean(TableColumn.ARCHIVED.index, archived);
            statement.setBoolean(TableColumn.DELETED.index, deleted);
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

    public static class Builder extends WriteQuery.Builder<Builder, InsertVisibilityQuery> {

        private String id;
        private Visibility entityStatus;

        public Builder setVisibility(Visibility status) {
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
        public InsertVisibilityQuery build() {
            checkNotNull(id, "ID is not set.");
            checkNotNull(entityStatus, "Entity status is not set.");
            return new InsertVisibilityQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }

    private enum TableColumn {

        ID(1),
        ARCHIVED(2),
        DELETED(3);

        private final int index;

        TableColumn(int index) {
            this.index = index;
        }
    }
}
