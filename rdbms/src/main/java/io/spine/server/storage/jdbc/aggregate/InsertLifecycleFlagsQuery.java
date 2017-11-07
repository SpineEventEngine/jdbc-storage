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

package io.spine.server.storage.jdbc.aggregate;

import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.archived;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.deleted;

/**
 * The query for creating a new record in the table storing
 * the {@linkplain LifecycleFlags entity lifecycle flags}.
 *
 * @author Dmytro Grankin
 */
class InsertLifecycleFlagsQuery<I> extends AbstractQuery implements WriteQuery {

    private final I id;
    private final LifecycleFlags entityStatus;
    private final IdColumn<I> idColumn;

    private InsertLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
        this.id = builder.id;
        this.entityStatus = builder.entityStatus;
        this.idColumn = builder.idColumn;
    }

    @Override
    public long execute() {
        return factory().insert(table())
                        .set(pathOf(idColumn.getColumnName()), idColumn.normalize(id))
                        .set(pathOf(archived), entityStatus.getArchived())
                        .set(pathOf(deleted), entityStatus.getDeleted())
                        .execute();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>, InsertLifecycleFlagsQuery> {

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
}
