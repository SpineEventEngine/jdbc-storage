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

import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.LifecycleFlagField;
import io.spine.server.storage.jdbc.LifecycleFlagsTable.Column;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static java.lang.String.format;

/**
 * The query selecting one {@linkplain LifecycleFlags entity lifecycle flags} by ID.
 *
 * @author Dmytro Dashenkov
 */
public class SelectLifecycleFlagsQuery<I> extends SelectMessageByIdQuery<I, LifecycleFlags> {

    private static final String SQL =
            SELECT.toString() + archived + COMMA + deleted +
            FROM + "%s" +
            WHERE + Column.id + EQUAL +
            PLACEHOLDER + SEMICOLON;

    private SelectLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    // Override the mechanism of the Message restoring
    @Override
    protected LifecycleFlags readMessage(ResultSet resultSet) throws SQLException {
        final boolean archived = resultSet.getBoolean(LifecycleFlagField.archived.toString());
        final boolean deleted = resultSet.getBoolean(LifecycleFlagField.deleted.toString());
        final LifecycleFlags visibility = LifecycleFlags.newBuilder()
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

    public static class Builder<I> extends SelectMessageByIdQuery.Builder<Builder<I>,
                                                                   SelectLifecycleFlagsQuery<I>,
                                                                   I,
                                                                   LifecycleFlags> {
        @Override
        public SelectLifecycleFlagsQuery<I> build() {
            return new SelectLifecycleFlagsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
