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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.record.RecordTable;

import static java.util.stream.Collectors.toList;

/**
 * Deletes multiple records by IDs from the {@link RecordTable}.
 *
 * //TODO:2022-01-10:alex.tymchenko: re-document.
 * //TODO:2022-01-10:alex.tymchenko: move this type.
 *
 * @param <I>
 *         the ID type
 * @param <R>
 *         the type of the records to delete
 */
public final class DeleteMessagesInBulk<I, R extends Message> extends AbstractQuery implements WriteQuery {

    private final ImmutableList<I> ids;
    private final IdColumn<I> idColumn;

    private DeleteMessagesInBulk(Builder<I, R> builder) {
        super(builder);
        this.ids = builder.ids;
        this.idColumn = builder.idColumn;
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        var normalizedIds = ids
                .stream()
                .map(idColumn::normalize)
                .collect(toList());
        var query = factory().delete(table())
                             .where(pathOf(idColumn).in(normalizedIds));
        return query.execute();
    }

    public static <I, R extends Message> Builder<I, R> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I, R extends Message>
            extends AbstractQuery.Builder<I, R, Builder<I, R>, DeleteMessagesInBulk<I, R>> {

        private ImmutableList<I> ids;
        private IdColumn<I> idColumn;

        public Builder<I, R> setIds(Iterable<I> ids) {
            this.ids = ImmutableList.copyOf(ids);
            return getThis();
        }

        public Builder<I, R> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        @Override
        protected Builder<I, R> getThis() {
            return this;
        }

        @Override
        protected DeleteMessagesInBulk<I, R> doBuild() {
            return new DeleteMessagesInBulk<>(this);
        }
    }
}
