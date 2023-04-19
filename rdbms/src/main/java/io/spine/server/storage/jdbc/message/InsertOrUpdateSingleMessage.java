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

package io.spine.server.storage.jdbc.message;

import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.sql.dml.SQLInsertClause;
import io.spine.server.storage.jdbc.query.MysqlExtensions;

final class InsertOrUpdateSingleMessage<I, M extends Message> extends WriteSingleMessage<I, M>  {

    private InsertOrUpdateSingleMessage(Builder<I, M> builder) {
        super(builder);
    }

    @Override
    protected StoreClause<?> clause() {
        SQLInsertClause original = factory().insert(table());
        SQLInsertClause optimized = MysqlExtensions.withOnDuplicateUpdate(original);
        SQLInsertClause result =  optimized.set(idPath(), normalizedId());
        return result;
    }

    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I, M extends Message>
            extends WriteSingleMessage.Builder<I,
                                               M,
                                               InsertOrUpdateSingleMessage.Builder<I, M>,
                                               InsertOrUpdateSingleMessage<I, M>> {

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected InsertOrUpdateSingleMessage<I, M> doBuild() {
            return new InsertOrUpdateSingleMessage<>(this);
        }
    }
}