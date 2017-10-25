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
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.LifecycleFlagsTable.Column;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SET;
import static io.spine.server.storage.jdbc.Sql.Query.UPDATE;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static java.lang.String.format;

/**
 * The query updating an {@linkplain LifecycleFlags entity lifecycle flags}.
 *
 * @author Dmytro Dashenkov
 */
public class UpdateLifecycleFlagsQuery<I> extends WriteQuery {

    private static final String SQL = UPDATE + "%s" + SET +
                                      archived + EQUAL + PLACEHOLDER + COMMA +
                                      deleted + EQUAL + PLACEHOLDER +
                                      WHERE + Column.id + EQUAL + PLACEHOLDER + SEMICOLON;

    private final I id;
    private final LifecycleFlags entityStatus;
    private final IdColumn<I> idColumn;

    protected UpdateLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
        this.id = builder.id;
        this.entityStatus = builder.entityStatus;
        this.idColumn = builder.idColumn;
    }

    @Override
    protected Parameters getQueryParameters() {
        final Parameters.Builder builder = Parameters.newBuilder();
        idColumn.setId(3, id, builder);

        final Parameter archived = Parameter.of(entityStatus.getArchived(), Column.archived);
        final Parameter deleted = Parameter.of(entityStatus.getDeleted(), Column.deleted);
        return builder.addParameter(1, archived)
                      .addParameter(2, deleted)
                      .build();
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(SQL, tableName));
        return builder;
    }

    public static class Builder<I> extends WriteQuery.Builder<Builder<I>, UpdateLifecycleFlagsQuery> {

        private I id;
        private LifecycleFlags entityStatus;
        private IdColumn<I> idColumn;

        public Builder<I> setId(I id) {
            this.id = checkNotNull(id);
            return getThis();
        }

        public Builder<I> setLifecycleFlags(LifecycleFlags status) {
            this.entityStatus = checkNotNull(status);
            return getThis();
        }

        @Override
        public UpdateLifecycleFlagsQuery build() {
            checkState(id != null, "ID is not set.");
            checkState(entityStatus != null, "Entity status is not set.");
            return new UpdateLifecycleFlagsQuery<>(this);
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
