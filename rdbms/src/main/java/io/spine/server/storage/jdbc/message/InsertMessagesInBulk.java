/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.message;

import com.google.protobuf.Message;
import com.querydsl.sql.dml.SQLInsertClause;

/**
 * Inserts multiple messages to the {@link MessageTable} in a batch.
 *
 * @param <I>
 *         the record ID type
 * @param <M>
 *         the message type
 */
final class InsertMessagesInBulk<I, M extends Message>
        extends WriteMessagesInBulk<I, M, SQLInsertClause> {

    private InsertMessagesInBulk(Builder<I, M> builder) {
        super(builder);
    }

    @Override
    protected SQLInsertClause clause() {
        return factory().insert(table());
    }

    @Override
    protected void setIdClause(SQLInsertClause query, I id, M record) {
        query.set(pathOf(idColumn()),
                  idColumn().normalize(id));
    }

    @Override
    protected void addBatch(SQLInsertClause query) {
        query.addBatch();
    }

    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I, M extends Message>
            extends WriteMessagesInBulk.Builder<I, M, Builder<I, M>, InsertMessagesInBulk<I, M>> {

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected InsertMessagesInBulk<I, M> doBuild() {
            return new InsertMessagesInBulk<>(this);
        }
    }
}
