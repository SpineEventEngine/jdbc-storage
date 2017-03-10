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
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.table.entity.aggregate.VisibilityTable.Column;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.*;
import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SET;
import static org.spine3.server.storage.jdbc.Sql.Query.UPDATE;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;

/**
 * The query updating an {@linkplain org.spine3.server.entity.Visibility entity visibility}.
 *
 * @author Dmytro Dashenkov.
 */
public class UpdateVisibilityQuery<I> extends WriteQuery {

    private static final String SQL = UPDATE + "%s" + SET +
                                      archived + EQUAL + PLACEHOLDER + COMMA +
                                      deleted + EQUAL + PLACEHOLDER +
                                      WHERE + Column.id + EQUAL + PLACEHOLDER + SEMICOLON;

    private final I id;
    private final Visibility entityStatus;
    private final IdColumn<I> idColumn;

    protected UpdateVisibilityQuery(Builder<I> builder) {
        super(builder);
        this.id = builder.id;
        this.entityStatus = builder.entityStatus;
        this.idColumn = builder.idColumn;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        final boolean archived = entityStatus.getArchived();
        final boolean deleted = entityStatus.getDeleted();
        try {
            statement.setBoolean(1, archived);
            statement.setBoolean(2, deleted);
            idColumn.setId(3, id, statement);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return statement;
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(SQL, tableName));
        return builder;
    }

    public static class Builder<I> extends WriteQuery.Builder<Builder<I>, UpdateVisibilityQuery> {

        private I id;
        private Visibility entityStatus;
        private IdColumn<I> idColumn;

        public Builder<I> setId(I id) {
            this.id = checkNotNull(id);
            return getThis();
        }

        public Builder<I> setVisibility(Visibility status) {
            this.entityStatus = checkNotNull(status);
            return getThis();
        }

        @Override
        public UpdateVisibilityQuery build() {
            checkState(id != null, "ID is not set.");
            checkState(entityStatus != null, "Entity status is not set.");
            return new UpdateVisibilityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
