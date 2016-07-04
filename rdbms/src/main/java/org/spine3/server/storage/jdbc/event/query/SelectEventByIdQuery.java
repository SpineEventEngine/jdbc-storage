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

package org.spine3.server.storage.jdbc.event.query;

import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;

import static org.spine3.server.storage.jdbc.event.query.EventTable.*;

/**
 * Query that selects {@link EventStorageRecord} by ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectEventByIdQuery extends SelectByIdQuery<String, EventStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_QUERY = SELECT_EVENT_FROM_TABLE +
                                               " WHERE " + EVENT_ID_COL + " = ?;";

    public SelectEventByIdQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setIdIndexInQuery(1)
                .setQuery(SELECT_QUERY)
                .setMessageColumnName(EVENT_COL)
                .setMessageDescriptor(EventStorageRecord.getDescriptor());
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends SelectByIdQuery.Builder<Builder, SelectEventByIdQuery, String, EventStorageRecord>{

        @Override
        public SelectEventByIdQuery build() {
            return new SelectEventByIdQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
