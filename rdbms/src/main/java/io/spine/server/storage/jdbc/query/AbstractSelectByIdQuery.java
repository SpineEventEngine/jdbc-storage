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

import io.spine.server.storage.jdbc.IdColumn;

/**
 * An abstract base for the queries which read a single record by ID.
 *
 * @author Dmytro Dashenkov
 */
abstract class AbstractSelectByIdQuery<I, R> extends AbstractQuery implements SelectByIdQuery<I, R> {

    private final IdColumn<I> idColumn;
    private final I id;
    private final int idIndexInQuery;

    protected AbstractSelectByIdQuery(Builder<I, ? extends Builder, ? extends StorageQuery> builder) {
        super(builder);
        this.id = builder.getId();
        this.idColumn = builder.getIdColumn();
        this.idIndexInQuery = builder.getIdIndexInQuery();
    }

    @Override
    public IdColumn<I> getIdColumn() {
        return idColumn;
    }

    @Override
    public I getId() {
        return id;
    }

    public int getIdIndexInQuery() {
        return idIndexInQuery;
    }

    protected abstract static class Builder<I,
                                            B extends Builder<I, B, Q>,
                                            Q extends AbstractSelectByIdQuery>
            extends AbstractQuery.Builder<B, Q> {

        private int idIndexInQuery;
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

        public B setIdIndexInQuery(int idIndexInQuery) {
            this.idIndexInQuery = idIndexInQuery;
            return getThis();
        }

        public int getIdIndexInQuery() {
            return idIndexInQuery;
        }

        public IdColumn<I> getIdColumn() {
            return idColumn;
        }

        public I getId() {
            return id;
        }
    }
}