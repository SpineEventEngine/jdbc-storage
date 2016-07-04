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

package org.spine3.server.storage.jdbc.command.query;

import org.spine3.base.CommandStatus;
import org.spine3.server.storage.jdbc.query.UpdateRecordQuery;

import static org.spine3.server.storage.jdbc.command.query.CommandTable.*;

/**
 * Query that sets {@link CommandStatus} to OK state.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SetOkStatusQuery extends UpdateRecordQuery<String> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SET_OK_STATUS_QUERY =
            "UPDATE " + TABLE_NAME +
            " SET " + COMMAND_STATUS_COL + " = '" + CommandStatus.forNumber(CommandStatus.OK_VALUE).name() + '\'' +
            " WHERE " + ID_COL + " = ? ;";

    private SetOkStatusQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setIdIndexInQuery(1)
               .setQuery(SET_OK_STATUS_QUERY);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends UpdateRecordQuery.Builder<Builder, SetOkStatusQuery, String> {

        @Override
        public SetOkStatusQuery build() {
            return new SetOkStatusQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
