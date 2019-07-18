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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.protobuf.AnyPacker;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

final class SelectMessageById<I, M extends Message>
        extends IdAwareQuery<I>
        implements SelectQuery<M> {

    private final TableColumn bytesColumn;
    private final TypeUrl typeUrl;

    private SelectMessageById(Builder<I, M> builder) {
        super(builder);
        this.bytesColumn = builder.bytesColumn;
        this.typeUrl = builder.typeUrl;
    }

    @Override
    public final @Nullable M execute() throws DatabaseException {
        try (ResultSet resultSet = query().getResults()) {
            if (!resultSet.next()) {
                return null;
            }
            M message = readMessage(resultSet);
            return message;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private AbstractSQLQuery<Object, ?> query() {
        return factory().select(pathOf(bytesColumn))
                        .from(table())
                        .where(idEquals());
    }

    @SuppressWarnings("unchecked") // It's up to user to specify the valid data for unpacking.
    private @Nullable M readMessage(ResultSet resultSet) throws SQLException {
        byte[] bytes = resultSet.getBytes(bytesColumn.name());
        if (bytes == null) {
            return null;
        }
        Any any = Any.newBuilder()
                     .setValue(ByteString.copyFrom(bytes))
                     .setTypeUrl(typeUrl.value())
                     .build();
        M result = (M) AnyPacker.unpack(any);
        return result;
    }

    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I, M extends Message>
            extends IdAwareQuery.Builder<I, Builder<I, M>, SelectMessageById<I, M>> {

        private TableColumn bytesColumn;
        private TypeUrl typeUrl;

        Builder<I, M> setBytesColumn(TableColumn bytesColumn) {
            this.bytesColumn = bytesColumn;
            return getThis();
        }

        Builder<I, M> setTypeUrl(TypeUrl typeUrl) {
            this.typeUrl = typeUrl;
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
