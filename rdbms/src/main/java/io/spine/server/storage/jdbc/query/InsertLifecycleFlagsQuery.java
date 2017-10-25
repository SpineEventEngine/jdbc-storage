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
import io.spine.server.storage.jdbc.Sql;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * The query for creating a new record in the table storing
 * the {@linkplain LifecycleFlags entity lifecycle flags}.
 *
 * @author Dmytro Dashenkov
 */
class InsertLifecycleFlagsQuery<I> extends WriteQuery {

    private static final int COLUMN_COUNT = TableColumn.values().length;
    private static final String SQL =
            Sql.Query.INSERT_INTO + "%s" +
            Sql.Query.VALUES + Sql.nPlaceholders(COLUMN_COUNT) +
            Sql.BuildingBlock.SEMICOLON;

    private final I id;
    private final LifecycleFlags entityStatus;
    private final IdColumn<I> idColumn;

    InsertLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
        this.id = builder.id;
        this.entityStatus = builder.entityStatus;
        this.idColumn = builder.idColumn;
    }

    @Override
    protected Parameters getQueryParameters() {
        final Parameters.Builder builder = Parameters.newBuilder();
        idColumn.setId(String.valueOf(TableColumn.ID.getIndex()), id, builder);

        final Parameter archived = Parameter.of(entityStatus.getArchived(), Column.archived);
        final Parameter deleted = Parameter.of(entityStatus.getDeleted(), Column.deleted);
        return builder.addParameter(String.valueOf(TableColumn.ARCHIVED.getIndex()), archived)
                      .addParameter(String.valueOf(TableColumn.DELETED.getIndex()), deleted)
                      .build();
    }

    static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setQuery(format(SQL, tableName));
        return builder;
    }

    static class Builder<I> extends WriteQuery.Builder<Builder<I>, InsertLifecycleFlagsQuery> {

        private I id;
        private LifecycleFlags entityStatus;
        private IdColumn<I> idColumn;

        Builder<I> setLifecycleFlags(LifecycleFlags status) {
            this.entityStatus = checkNotNull(status);
            return getThis();
        }

        Builder<I> setId(I id) {
            this.id = checkNotNull(id);
            return getThis();
        }

        @Override
        public InsertLifecycleFlagsQuery build() {
            checkNotNull(id, "ID is not set.");
            checkNotNull(entityStatus, "Entity status is not set.");
            return new InsertLifecycleFlagsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
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

        public int getIndex() {
            return index;
        }
    }
}
