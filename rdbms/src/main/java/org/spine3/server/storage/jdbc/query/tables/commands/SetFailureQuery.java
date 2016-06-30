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

package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.base.Failure;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;


public class SetFailureQuery extends WriteRecord<String, Failure> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SET_FAILURE_QUERY =
            "UPDATE " + CommandTable.TABLE_NAME +
                    " SET " +
                    CommandTable.FAILURE_COL + " = ? " +
                    " WHERE " + CommandTable.ID_COL + " = ? ;";

    private SetFailureQuery(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setIdIndexInQuery(2)
                .setRecordIndexInQuery(1)
                .setQuery(SET_FAILURE_QUERY);
        return builder;
    }

    public static class Builder extends WriteRecord.Builder<Builder, SetFailureQuery, String, Failure> {

        @Override
        public SetFailureQuery build() {
            return new SetFailureQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
