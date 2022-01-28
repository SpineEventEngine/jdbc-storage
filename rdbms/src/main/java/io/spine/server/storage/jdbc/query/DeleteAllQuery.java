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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;

/**
 * A query that deletes all records from a table.
 *
 * //TODO:2021-06-24:alex.tymchenko: move this type?
 *
 * @param <I>
 *         the type of the record identifiers
 * @param <R>
 *         the type of the records to delete
 */
public class DeleteAllQuery<I, R extends Message>
        extends AbstractQuery<I, R>
        implements WriteQuery {

    private DeleteAllQuery(Builder<I, R> builder) {
        super(builder);
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        var query = factory().delete(table());
        return query.execute();
    }

    /**
     * Creates a new {@code Builder} for this query type.
     */
    public static <I, R extends Message> Builder<I, R> newBuilder() {
        return new Builder<>();
    }

    /**
     * A builder of {@code DeleteAllQuery} instances.
     */
    public static class Builder<I, R extends Message>
            extends AbstractQuery.Builder<I, R, Builder<I, R>, DeleteAllQuery<I, R>> {

        @Override
        protected DeleteAllQuery<I, R> doBuild() {
            return new DeleteAllQuery<>(this);
        }

        @Override
        protected Builder<I, R> getThis() {
            return this;
        }
    }
}
