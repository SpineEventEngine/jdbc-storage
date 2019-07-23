/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;

import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;

/**
 * Selects a single message from the {@link MessageTable} by its ID.
 *
 * @param <I>
 *         the ID type
 * @param <M>
 *         the message type
 */
final class SelectSingleMessage<I, M extends Message> extends SelectMessageByIdQuery<I, M> {

    private SelectSingleMessage(Builder<I, M> builder) {
        super(builder);
    }

    @Override
    protected AbstractSQLQuery<Object, ?> query() {
        return factory().select(pathOf(bytesColumn()))
                        .from(table())
                        .where(idEquals());
    }

    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I, M extends Message>
            extends SelectMessageByIdQuery.Builder<Builder<I, M>, SelectSingleMessage<I, M>, I, M> {

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected SelectSingleMessage<I, M> doBuild() {
            setMessageColumnName(bytesColumn().name());
            return new SelectSingleMessage<>(this);
        }
    }
}
