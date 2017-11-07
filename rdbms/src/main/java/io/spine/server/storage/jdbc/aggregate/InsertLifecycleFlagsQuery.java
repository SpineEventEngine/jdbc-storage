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
import io.spine.server.storage.jdbc.query.IdAwareQuery;
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
class InsertLifecycleFlagsQuery<I> extends IdAwareQuery<I> implements WriteQuery {

    private final LifecycleFlags entityStatus;

    private InsertLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
        this.entityStatus = builder.entityStatus;
    }

    @Override
    public long execute() {
        return factory().insert(table())
                        .set(idPath(), getNormalizedId())
                        .set(pathOf(archived), entityStatus.getArchived())
                        .set(pathOf(deleted), entityStatus.getDeleted())
                        .execute();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends IdAwareQuery.Builder<I, Builder<I>, InsertLifecycleFlagsQuery<I>> {

        private LifecycleFlags entityStatus;

        Builder<I> setLifecycleFlags(LifecycleFlags status) {
            this.entityStatus = checkNotNull(status);
            return getThis();
        }

        @Override
        public InsertLifecycleFlagsQuery<I> build() {
            checkNotNull(entityStatus, "Entity status is not set.");
            return new InsertLifecycleFlagsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
