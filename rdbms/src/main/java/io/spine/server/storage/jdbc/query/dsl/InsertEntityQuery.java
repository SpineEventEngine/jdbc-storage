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

import com.querydsl.core.dml.StoreClause;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.RecordTable;
import io.spine.server.storage.jdbc.query.Parameter;
import io.spine.server.storage.jdbc.query.Parameters;

/**
 * Query that inserts a new {@link EntityRecordWithColumns} to
 * the {@link RecordTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 * @author Alexander Aleksandrov
 */
class InsertEntityQuery<I> extends WriteEntityQuery<I> {

    private InsertEntityQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    StoreClause<?> createClause() {
        return factory().insert(table());
    }

    @Override
    Parameters getParameters() {
        final Parameters superParameters = super.getParameters();
        final Object normalizedId = getIdColumn().normalize(getId());
        final Parameter idParameter = Parameter.of(normalizedId, getIdColumn().getSqlType());
        return Parameters.newBuilder()
                         .addParameters(superParameters)
                         .addParameter(getIdColumn().getColumnName(), idParameter)
                         .build();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends WriteRecordQuery.Builder<Builder<I>,
                                                             InsertEntityQuery,
                                                             I,
                                                             EntityRecordWithColumns> {

        @Override
        InsertEntityQuery build() {
            return new InsertEntityQuery<>(this);
        }

        @Override
        Builder<I> getThis() {
            return this;
        }
    }
}