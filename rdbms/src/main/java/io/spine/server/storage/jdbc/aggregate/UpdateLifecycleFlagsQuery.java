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

import com.querydsl.sql.dml.SQLUpdateClause;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.ARCHIVED;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.DELETED;

/**
 * The query updating {@linkplain LifecycleFlags entity lifecycle flags}.
 *
 * @author Dmytro Grankin
 */
class UpdateLifecycleFlagsQuery<I> extends IdAwareQuery<I> implements WriteQuery {

    private final LifecycleFlags entityStatus;

    private UpdateLifecycleFlagsQuery(Builder<I> builder) {
        super(builder);
        this.entityStatus = builder.entityStatus;
    }

    @Override
    public long execute() {
        SQLUpdateClause query = factory().update(table())
                                         .where(idPath().eq(getNormalizedId()))
                                         .set(pathOf(ARCHIVED), entityStatus.getArchived())
                                         .set(pathOf(DELETED), entityStatus.getDeleted());
        return query.execute();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I>
            extends IdAwareQuery.Builder<I, Builder<I>, UpdateLifecycleFlagsQuery<I>> {

        private LifecycleFlags entityStatus;

        Builder<I> setLifecycleFlags(LifecycleFlags status) {
            this.entityStatus = checkNotNull(status);
            return getThis();
        }

        /**
         * {@inheritDoc}
         *
         * <p>Also checks that {@link LifecycleFlags} were set.
         */
        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkState(entityStatus != null, "Entity status is not set.");
        }

        @Override
        protected UpdateLifecycleFlagsQuery<I> doBuild() {
            return new UpdateLifecycleFlagsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
