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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.ColumnReader;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

final class SelectMessageById<I, M extends Message>
        extends IdAwareQuery<I>
        implements SelectQuery<M> {

    private final Descriptor messageDescriptor;

    private SelectMessageById(Builder<I, M> builder) {
        super(builder);
        this.messageDescriptor = builder.messageDescriptor;
    }

    @Override
    public final @Nullable M execute() throws DatabaseException {
        try (ResultSet resultSet = query().getResults()) {
            if (!resultSet.next()) {
                return null;
            }
            ColumnReader<M> reader = messageReader(bytesColumn().name(), messageDescriptor);
            M result = reader.readValue(resultSet);
            return result;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private AbstractSQLQuery<Object, ?> query() {
        return factory().select(pathOf(bytesColumn()))
                        .from(table())
                        .where(idEquals());
    }

    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I, M extends Message>
            extends IdAwareQuery.Builder<I, Builder<I, M>, SelectMessageById<I, M>> {

        private Descriptor messageDescriptor;

        Builder<I, M> setMessageDescriptor(Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected SelectMessageById<I, M> doBuild() {
            return new SelectMessageById<>(this);
        }
    }
}
