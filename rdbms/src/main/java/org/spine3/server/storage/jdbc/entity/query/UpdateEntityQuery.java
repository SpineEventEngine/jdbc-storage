/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.entity.query;

import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.entity.query.Constants.ENTITY_COL;
import static org.spine3.server.storage.jdbc.entity.query.Constants.ID_COL;


public class UpdateEntityQuery<I> extends WriteRecordQuery<I, EntityStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String UPDATE_QUERY =
            "UPDATE %s " +
                    " SET " + ENTITY_COL + " = ? " +
                    " WHERE " + ID_COL + " = ?;";

    private UpdateEntityQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I> Builder <I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setIdIndexInQuery(2)
                .setRecordIndexInQuery(1)
                .setQuery(format(UPDATE_QUERY, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends WriteRecordQuery.Builder<Builder<I>, UpdateEntityQuery, I, EntityStorageRecord> {

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
