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
import org.spine3.server.storage.EntityStatusField;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.server.storage.EntityStatusField.archived;
import static org.spine3.server.storage.EntityStatusField.deleted;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.ID_COL;
import static org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable.TABLE_NAME;

/**
 * @author Dmytro Dashenkov.
 */
public class SelectEntityStatusQuery extends StorageQuery {

    private static final String SQL =
            SELECT.toString() + archived + COMMA + deleted +
            FROM + TABLE_NAME +
            WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    private final String id;

    protected SelectEntityStatusQuery(Builder builder) {
        super(builder);
        this.id = builder.id;
    }

    public EntityStatus execute() {
        final boolean archived;
        final boolean deleted;
        try (ConnectionWrapper connection = getConnection(false)) {
            final PreparedStatement statement = prepareStatement(connection);
            statement.setString(1, id);
            final ResultSet resultSet = statement.executeQuery();
            final boolean empty = !resultSet.next();
            if (empty) {
                return EntityStatus.getDefaultInstance();
            }
            archived = resultSet.getBoolean(EntityStatusField.archived.toString());
            deleted = resultSet.getBoolean(EntityStatusField.deleted.toString());
            statement.close();
            resultSet.close();
        } catch (SQLException e) {
            getLogger().error("Failed to read EntityStatus.", e);
            throw new DatabaseException(e);
        }
        final EntityStatus status = EntityStatus.newBuilder()
                                                .setArchived(archived)
                                                .setDeleted(deleted)
                                                .build();
        return status;
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(SQL);
        return builder;
    }

    public static class Builder extends StorageQuery.Builder<Builder, SelectEntityStatusQuery> {

        private String id;

        public Builder setId(Object id) {
            checkNotNull(id);
            final String stringId = idToString(id);
            this.id = stringId;
            return getThis();
        }

        @Override
        public SelectEntityStatusQuery build() {
            return new SelectEntityStatusQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
