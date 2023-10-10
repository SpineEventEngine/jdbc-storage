/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Message;
import com.querydsl.sql.dml.SQLInsertClause;
import io.spine.server.storage.jdbc.record.RecordTable;

/**
 * Inserts multiple records to the {@link RecordTable} in a batch.
 *
 * @param <I>
 *         the record ID type
 * @param <R>
 *         the record type
 */
public class InsertMultipleQuery<I, R extends Message>
        extends WriteMultipleQuery<I, R, SQLInsertClause> {

    private InsertMultipleQuery(Builder<I, R> builder) {
        super(builder);
    }

    @Override
    protected SQLInsertClause clause() {
        return factory().insert(table());
    }

    @Override
    protected void setIdClause(SQLInsertClause query, I id) {
        query.set(pathOf(idColumn()),
                  idColumn().normalize(id));
    }

    @Override
    protected void addBatch(SQLInsertClause query) {
        query.addBatch();
    }

    public static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName" /* For simplicity. */)
    public static class Builder<I, M extends Message>
            extends WriteMultipleQuery.Builder<I, M, Builder<I, M>, InsertMultipleQuery<I, M>> {

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected InsertMultipleQuery<I, M> doBuild() {
            return new InsertMultipleQuery<>(this);
        }
    }
}
