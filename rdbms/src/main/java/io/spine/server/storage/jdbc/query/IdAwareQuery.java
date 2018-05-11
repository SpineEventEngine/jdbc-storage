/*
 * Copyright 2017, TeamDev. All rights reserved.
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

import com.querydsl.core.types.dsl.PathBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract base for queries, which work with a {@link IdColumn single ID}.
 *
 * @param <I> the ID type
 * @author Dmytro Grankin
 */
public abstract class IdAwareQuery<I> extends AbstractQuery {

    private final I id;
    private final IdColumn<I> idColumn;

    protected IdAwareQuery(Builder<I, ? extends Builder, ? extends IdAwareQuery> builder) {
        super(builder);
        this.id = checkNotNull(builder.id);
        this.idColumn = checkNotNull(builder.idColumn);
    }

    protected PathBuilder<Object> idPath() {
        return pathOf(idColumn.getColumnName());
    }

    protected Object getNormalizedId() {
        return idColumn.normalize(id);
    }

    protected abstract static class Builder<I,
                                            B extends IdAwareQuery.Builder<I, B, Q>,
                                            Q extends IdAwareQuery<I>>
            extends AbstractQuery.Builder<B, Q> {

        private IdColumn<I> idColumn;
        private I id;

        public B setId(I id) {
            this.id = id;
            return getThis();
        }

        public B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
