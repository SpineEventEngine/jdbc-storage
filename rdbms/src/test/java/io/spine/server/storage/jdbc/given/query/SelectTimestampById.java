/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import com.google.protobuf.Timestamp;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;

import java.sql.ResultSet;

import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;

/**
 * Selects a {@link Timestamp} by ID from the
 * {@linkplain io.spine.server.storage.jdbc.message.MessageTable message table}.
 *
 * @param <I>
 *         the type of IDs
 */
public class SelectTimestampById<I> extends SelectMessageByIdQuery<I, Timestamp> {

    private SelectTimestampById(Builder<I> builder) {
        super(builder);
    }

    public ResultSet getResults() {
        ResultSet results = query().getResults();
        return results;
    }

    @Override
    public AbstractSQLQuery<Object, ?> query() {
        return factory().select(pathOf(bytesColumn()))
                        .from(table())
                        .where(idEquals());
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I>
            extends SelectMessageByIdQuery.Builder<Builder<I>,
                                                   SelectTimestampById<I>,
                                                   I,
                                                   Timestamp> {

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        @Override
        protected SelectTimestampById<I> doBuild() {
            setMessageColumnName(bytesColumn().name());
            return new SelectTimestampById<>(this);
        }
    }
}
