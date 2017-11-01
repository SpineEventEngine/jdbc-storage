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

import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.AbstractStoreQuery;
import io.spine.server.storage.jdbc.query.Parameter;
import io.spine.server.storage.jdbc.query.Parameters;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.archived;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.deleted;

/**
 * The query updating {@linkplain LifecycleFlags entity lifecycle flags}.
 *
 * @author Dmytro Dashenkov
 */
class UpdateLifecycleFlagsQuery<I> extends AbstractStoreQuery {

    private final I id;
    private final LifecycleFlags entityStatus;
    private final IdColumn<I> idColumn;

    private UpdateLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
        this.id = builder.id;
        this.entityStatus = builder.entityStatus;
        this.idColumn = builder.idColumn;
    }

    @Override
    protected StoreClause<?> createClause() {
        final PathBuilder<Object> idPath = pathOf(idColumn.getColumnName());
        final Object normalizedId = idColumn.normalize(id);
        return factory().update(table())
                        .where(idPath.eq(normalizedId));
    }

    @Override
    protected Parameters getParameters() {
        final Parameters.Builder builder = Parameters.newBuilder();
        final Parameter archivedParameter = Parameter.of(entityStatus.getArchived());
        final Parameter deletedParameter = Parameter.of(entityStatus.getDeleted());
        return builder.addParameter(archived.name(), archivedParameter)
                      .addParameter(deleted.name(), deletedParameter)
                      .build();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractStoreQuery.Builder<Builder<I>, UpdateLifecycleFlagsQuery> {

        private I id;
        private LifecycleFlags entityStatus;
        private IdColumn<I> idColumn;

        Builder<I> setId(I id) {
            this.id = checkNotNull(id);
            return getThis();
        }

        Builder<I> setLifecycleFlags(LifecycleFlags status) {
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

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
