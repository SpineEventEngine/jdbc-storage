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

package io.spine.server.storage.jdbc.query.dsl;

import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.Parameter;
import io.spine.server.storage.jdbc.query.Parameters;

abstract class WriteRecordQuery<I, R> extends ColumnAwareWriteQuery {

    private final I id;
    private final EntityRecordWithColumns record;
    private final IdColumn<I> idColumn;

    WriteRecordQuery(Builder<? extends Builder, ? extends WriteRecordQuery, I, R> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
        this.record = builder.record;
    }

    EntityRecordWithColumns getRecord() {
        return record;
    }

    I getId() {
        return id;
    }

    IdColumn<I> getIdColumn() {
        return idColumn;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q, I, R>,
                                  Q extends WriteRecordQuery,
                                  I,
                                  R>
            extends ColumnAwareWriteQuery.Builder<B, Q> {

        private IdColumn<I> idColumn;
        private I id;
        private EntityRecordWithColumns record;

        B setId(I id) {
            this.id = id;
            return getThis();
        }

        B setRecord(EntityRecordWithColumns record) {
            this.record = record;
            return getThis();
        }

        B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
