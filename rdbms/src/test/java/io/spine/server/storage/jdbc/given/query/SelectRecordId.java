/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.given.query;

import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.query.ReadByIdQuery;
import io.spine.server.storage.jdbc.record.RecordTable;

import java.sql.ResultSet;

/**
 * A test query for selecting a {@code Message} ID from the {@link RecordTable}.
 *
 * <p>Although selecting a record ID by ID is hardly a viable case in real life,
 * it's sometimes necessary in tests.
 *
 * <p>The query result is returned as a {@link ResultSet}.
 *
 * @param <I>
 *         the ID type
 * @param <R>
 *         the record type
 */
public final class SelectRecordId<I, R extends Message> extends ReadByIdQuery<I, R> {

    private SelectRecordId(Builder<I, R> builder) {
        super(builder);
    }

    public ResultSet getResults() {
        var results = query().getResults();
        return results;
    }

    private AbstractSQLQuery<Object, ?> query() {
        return factory().select(pathOf(idColumn().column()))
                        .from(table())
                        .where(idEquals());
    }

    public static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I, R extends Message>
            extends ReadByIdQuery.Builder<I, R, Builder<I, R>, SelectRecordId<I, R>> {

        @Override
        protected Builder<I, R> getThis() {
            return this;
        }

        @Override
        protected SelectRecordId<I, R> doBuild() {
            return new SelectRecordId<>(this);
        }
    }
}
