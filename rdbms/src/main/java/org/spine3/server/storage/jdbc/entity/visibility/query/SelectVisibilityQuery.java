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
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.table.entity.aggregate.VisibilityTable.Column;

/**
 * The query selecting one {@linkplain org.spine3.server.entity.Visibility entity visibility} by ID.
 *
 * @author Dmytro Dashenkov.
 */
public class SelectVisibilityQuery<I> extends SelectByIdQuery<I, Visibility> {

    private static final String SQL =
            SELECT.toString() + archived + COMMA + deleted +
            FROM + "%s" +
            WHERE + Column.id + EQUAL + PLACEHOLDER + SEMICOLON;

    protected SelectVisibilityQuery(Builder<I> builder) {
        super(builder);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    // Override the mechanism of the Message restoring
    @Override
    protected Visibility readMessage(ResultSet resultSet) throws SQLException {
        final boolean archived = resultSet.getBoolean(VisibilityField.archived.toString());
        final boolean deleted = resultSet.getBoolean(VisibilityField.deleted.toString());
        final Visibility visibility = Visibility.newBuilder()
                                                .setArchived(archived)
                                                .setDeleted(deleted)
                                                .build();
        return visibility;
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(SQL, tableName))
               .setIdIndexInQuery(1);
        return builder;
    }

    public static class Builder<I> extends SelectByIdQuery.Builder<Builder<I>,
                                                                   SelectVisibilityQuery,
                                                                   I,
                                                                   Visibility> {
        @Override
        public SelectVisibilityQuery<I> build() {
            return new SelectVisibilityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
