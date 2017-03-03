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

import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.VisibilityField;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable.ID_COL;
import static org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable.TABLE_NAME;

/**
 * The query selecting one {@linkplain org.spine3.server.entity.Visibility entity visibility} by ID.
 *
 * @author Dmytro Dashenkov.
 */
public class SelectVisibilityQuery extends StorageQuery {

    private static final String SQL =
            SELECT.toString() + archived + COMMA + deleted +
            FROM + TABLE_NAME +
            WHERE + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    private final String id;

    protected SelectVisibilityQuery(Builder builder) {
        super(builder);
        this.id = builder.id;
    }

    @Nullable
    public Visibility execute() {
        final boolean archived;
        final boolean deleted;
        try (ConnectionWrapper connection = getConnection(false);
             PreparedStatement statement = prepareStatement(connection)) {
            statement.setString(1, id);
            final ResultSet resultSet = statement.executeQuery();
            if (!resultSet.next()) { // Empty result set
                return null;
            }
            archived = resultSet.getBoolean(VisibilityField.archived.toString());
            deleted = resultSet.getBoolean(VisibilityField.deleted.toString());
            resultSet.close();
        } catch (SQLException e) {
            getLogger().error("Failed to read Visibility.", e);
            throw new DatabaseException(e);
        }
        final Visibility status = Visibility.newBuilder()
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

    public static class Builder extends StorageQuery.Builder<Builder, SelectVisibilityQuery> {

        private String id;

        public Builder setId(Object id) {
            checkNotNull(id);
            final String stringId = idToString(id);
            this.id = stringId;
            return getThis();
        }

        @Override
        public SelectVisibilityQuery build() {
            return new SelectVisibilityQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
