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

import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.RecordTable;

import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SET;
import static io.spine.server.storage.jdbc.Sql.Query.UPDATE;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.entity;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.id;
import static java.lang.String.format;

/**
 * Query that updates {@link EntityRecord} in
 * the {@link RecordTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class UpdateEntityQuery<I> extends WriteEntityQuery<I> {

    private static final int ID_INDEX = 2;
    private static final int RECORD_INDEX = 1;

    private static final String QUERY_TEMPLATE =
            UPDATE + "%s" +
            SET + entity + EQUAL + PLACEHOLDER +
            WHERE + id + EQUAL + PLACEHOLDER + SEMICOLON;

    private UpdateEntityQuery(Builder<I> builder) {
        super(builder);
    }

    static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setIdIndexInQuery(ID_INDEX)
               .setRecordIndexInQuery(RECORD_INDEX)
               .setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends WriteRecordQuery.Builder<Builder<I>,
                                                             UpdateEntityQuery,
                                                             I,
                                                             EntityRecordWithColumns> {
        @Override
        public UpdateEntityQuery build() {
            return new UpdateEntityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}