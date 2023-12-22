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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.Expression;
import com.querydsl.sql.SQLExpressions;

import static com.google.common.collect.Streams.stream;

/**
 * Inserts or updates a single record in the MySQL database, by the record ID.
 *
 * <p>This query uses a MySQL-specific {@code INSERT ... ON DUPLICATE KEY UPDATE} syntax.
 * In case the record by the provided ID already exists in the table, this query executes the
 * row update. Otherwise, it inserts a new table row reflecting the provided record.
 *
 * @param <I>
 *         the record ID type
 * @param <R>
 *         the record type
 */
public class MySqlUpsertOneQuery<I, R extends Message> extends WriteOneQuery<I, R> {

    private MySqlUpsertOneQuery(Builder<I, R> builder) {
        super(builder);
    }

    @Override
    protected StoreClause<?> clause() {
        var record = record();
        var expressions = stream(record.columns())
                .map((colName) ->
                             SQLExpressions.set(pathOf(colName), record.columnValue(colName)))
                .toArray(Expression[]::new);
        var result = mySqlFactory().insertOnDuplicateKeyUpdate(table(), expressions)
                                   .set(idPath(), normalizedId());
        return result;
    }

    public static <I, R extends Message> Builder<I, R> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName" /* For simplicity. */)
    public static class Builder<I, R extends Message>
            extends WriteOneQuery.Builder<I, R, Builder<I, R>, MySqlUpsertOneQuery<I, R>> {

        @Override
        protected Builder<I, R> getThis() {
            return this;
        }

        @Override
        protected MySqlUpsertOneQuery<I, R> doBuild() {
            return new MySqlUpsertOneQuery<>(this);
        }
    }
}
