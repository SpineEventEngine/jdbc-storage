/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.common.collect.ImmutableSet;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.PathBuilder;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract base for queries, which operate on a fixed set of {@link IdColumn IDs}.
 *
 * @param <I>
 *         the ID type
 */
public abstract class IdAwareQuery<I> extends AbstractQuery {

    private final ImmutableSet<I> ids;
    private final IdColumn<I> idColumn;

    protected IdAwareQuery(Builder<I, ? extends Builder, ? extends IdAwareQuery> builder) {
        super(builder);
        this.ids = checkNotNull(builder.ids);
        this.idColumn = checkNotNull(builder.idColumn);
    }

    /**
     * Adds the ID binding to the given {@code INSERT} or {@code UPDATE} clause.
     *
     * @throws IllegalArgumentException
     *         if multiple IDs were specified for the query in {@code Builder}
     */
    protected <C extends StoreClause<C>> C insertId(C query) {
        checkArgument(hasSingleId(),
                      "Record modification queries only accept a single ID as input");
        return query.set(idPath(), normalizedIds());
    }

    /**
     * Returns a {@code Predicate} to check if the value of the ID column matches the stored
     * set of IDs.
     */
    protected Predicate idMatches() {
        return idPath().in(normalizedIds());
    }

    private boolean hasSingleId() {
        return ids.size() == 1;
    }

    private Collection<Object> normalizedIds() {
        return idColumn.normalize(ids);
    }

    private PathBuilder<Object> idPath() {
        return pathOf(idColumn.columnName());
    }

    protected abstract static class Builder<I,
                                            B extends IdAwareQuery.Builder<I, B, Q>,
                                            Q extends IdAwareQuery<I>>
            extends AbstractQuery.Builder<B, Q> {

        private IdColumn<I> idColumn;
        private ImmutableSet<I> ids;

        public B setId(I id) {
            this.ids = ImmutableSet.of(id);
            return getThis();
        }

        public B setIds(Iterable<I> ids) {
            this.ids = ImmutableSet.copyOf(ids);
            return getThis();
        }

        public B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
