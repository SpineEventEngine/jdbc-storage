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

import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.entity.EntityRecord;

import static com.querydsl.sql.SQLExpressions.all;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.entity;

/**
 * Query that selects {@link EntityRecord} by ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class SelectEntityByIdQuery<I> extends SelectMessageByIdQuery<I, EntityRecord> {

    private SelectEntityByIdQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    AbstractSQLQuery<?, ?> getQuery() {
        return factory().select(all)
                        .from(table())
                        .where(hasId());
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends SelectMessageByIdQuery.Builder<Builder<I>,
                                                                   SelectEntityByIdQuery<I>,
                                                                   I,
                                                                   EntityRecord> {
        @Override
        public SelectEntityByIdQuery<I> build() {
            setMessageColumnName(entity.name());
            setMessageDescriptor(EntityRecord.getDescriptor());
            return new SelectEntityByIdQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
