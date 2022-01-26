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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Message;

import static com.querydsl.sql.SQLExpressions.count;

/**
 * A query that checks if the table contains a record with the given ID.
 */
//TODO:2021-06-18:alex.tymchenko: move this type.
public final class ContainsQuery<I, R extends Message>
        extends IdAwareQuery<I, R>
        implements SelectQuery<Boolean> {

    private ContainsQuery(Builder<I, R> builder) {
        super(builder);
    }

    /**
     * Returns {@code true} if there is at least one record with given ID, {@code} false otherwise.
     */
    @Override
    public Boolean execute() {
        var query = factory().select(count())
                             .from(table())
                             .where(idEquals());
        long recordsCount = query.fetchOne();
        return recordsCount > 0;
    }

    public static <I, R extends Message> Builder<I, R> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I, R extends Message>
            extends IdAwareQuery.Builder<I, R, Builder<I, R>, ContainsQuery<I, R>> {

        @Override
        protected ContainsQuery<I, R> doBuild() {
            return new ContainsQuery<>(this);
        }

        @Override
        protected Builder<I, R> getThis() {
            return this;
        }
    }
}
