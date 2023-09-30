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

/**
 * A query for deleting one or many items by an ID.
 *
 * @param <I>
 *         type of identifier of the record to delete
 * @param <R>
 *         type of the record to delete
 */
public final class DeleteRecordQuery<I, R extends Message>
        extends IdAwareQuery<I, R> implements ModifyQuery {

    private DeleteRecordQuery(Builder<I, R> builder) {
        super(builder);
    }

    @Override
    public long execute() {
        var query = factory().delete(table())
                             .where(idEquals());
        return query.execute();
    }

    /**
     * Creates a new instance of the {@code Builder} for this type.
     *
     * @param <I>
     *         type of identifier of the record to delete
     * @param <R>
     *         type of the record to delete
     */
    public static <I, R extends Message> Builder<I, R> newBuilder() {
        return new Builder<>();
    }

    /**
     * A builder of {@code DeleteRecordQuery}.
     *
     * @param <I>
     *         the type of identifiers of the records to delete
     */
    public static class Builder<I, R extends Message>
            extends IdAwareQuery.Builder<I, R, Builder<I, R>, DeleteRecordQuery<I, R>> {

        @Override
        protected DeleteRecordQuery<I, R> doBuild() {
            return new DeleteRecordQuery<>(this);
        }

        @Override
        protected Builder<I, R> getThis() {
            return this;
        }
    }
}
