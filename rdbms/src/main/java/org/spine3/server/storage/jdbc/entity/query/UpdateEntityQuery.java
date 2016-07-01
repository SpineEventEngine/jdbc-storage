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
import org.spine3.server.storage.jdbc.query.WriteRecord;
import static org.spine3.server.storage.jdbc.entity.query.Constants.*;

import static java.lang.String.format;


public class UpdateEntityQuery<Id> extends WriteRecord<Id, EntityStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String UPDATE_QUERY =
            "UPDATE %s " +
                    " SET " + ENTITY_COL + " = ? " +
                    " WHERE " + ID_COL + " = ?;";

    private UpdateEntityQuery(Builder<Id> builder) {
        super(builder);
    }


    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static <Id> Builder <Id> newBuilder(String tableName) {
        final Builder<Id> builder = new Builder<>();
        builder.setIdIndexInQuery(2)
                .setRecordIndexInQuery(1)
                .setQuery(format(UPDATE_QUERY, tableName));
        return builder;
    }

    public static class Builder<Id> extends WriteRecord.Builder<Builder<Id>, UpdateEntityQuery, Id, EntityStorageRecord> {

        @Override
        public UpdateEntityQuery build() {
            return new UpdateEntityQuery<>(this);
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
