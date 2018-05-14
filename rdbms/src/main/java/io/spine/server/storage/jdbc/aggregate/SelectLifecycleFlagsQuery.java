/*
 * Copyright 2018, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.aggregate;

import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.ARCHIVED;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.DELETED;

/**
 * The query selecting one {@linkplain LifecycleFlags entity lifecycle flags} by ID.
 *
 * @author Dmytro Grankin
 */
class SelectLifecycleFlagsQuery<I> extends SelectMessageByIdQuery<I, LifecycleFlags> {

    private SelectLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    protected AbstractSQLQuery<?, ?> getQuery() {
        final AbstractSQLQuery<?, ?> query = factory().select(pathOf(ARCHIVED), pathOf(DELETED))
                                                      .from(table())
                                                      .where(hasId());
        return query;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    // Override the mechanism of the Message restoring
    @Override
    protected LifecycleFlags readMessage(ResultSet resultSet) throws SQLException {
        final boolean archived = resultSet.getBoolean(Column.ARCHIVED.name());
        final boolean deleted = resultSet.getBoolean(Column.DELETED.name());
        final LifecycleFlags visibility = LifecycleFlags.newBuilder()
                                                        .setArchived(archived)
                                                        .setDeleted(deleted)
                                                        .build();
        return visibility;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends SelectMessageByIdQuery.Builder<Builder<I>,
                                                                   SelectLifecycleFlagsQuery<I>,
                                                                   I,
                                                                   LifecycleFlags> {

        @Override
        protected SelectLifecycleFlagsQuery<I> doBuild() {
            return new SelectLifecycleFlagsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
