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

import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;

public class UpdateRecordQuery<I> extends WriteQuery {

    private final I id;
    private final int idIndexInQuery;
    private final IdColumn<I> idColumn;

    protected UpdateRecordQuery(
            Builder<? extends Builder, ? extends UpdateRecordQuery, I> builder) {
        super(builder);
        this.idIndexInQuery = builder.idIndexInQuery;
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        idColumn.setId(idIndexInQuery, id, statement);
        return statement;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public abstract static class Builder<B extends Builder<B, Q, I>, Q extends UpdateRecordQuery, I>
            extends WriteQuery.Builder<B, Q> {
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
    }
}