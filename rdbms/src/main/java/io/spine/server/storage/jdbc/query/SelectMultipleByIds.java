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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.record.RecordTable;

import java.util.Iterator;

import static io.spine.server.storage.jdbc.query.reader.ColumnReaderFactory.messageReader;
import static io.spine.server.storage.jdbc.record.column.BytesColumn.bytesColumnName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Selects multiple records from the {@link RecordTable} by their IDs.
 *
 * //TODO:2021-06-18:alex.tymchenko: move this type.
 *
 * @param <I>
 *         the ID type
 * @param <R>
 *         the record type
 */
public final class SelectMultipleByIds<I, R extends Message>
        extends AbstractQuery<I, R>
        implements SelectQuery<Iterator<R>> {

    private final ImmutableList<I> ids;
    private final Descriptor messageDescriptor;

    private SelectMultipleByIds(Builder<I, R> builder) {
        super(builder);
        this.ids = builder.ids;
        this.messageDescriptor = requireNonNull(builder.tableSpec()).recordDescriptor();
    }

    @Override
    public Iterator<R> execute() {
        var results = query().getResults();
        DbIterator<R> iterator =
                DbIterator.over(results,
                                messageReader(bytesColumnName(), messageDescriptor));
        var list = ImmutableList.copyOf(iterator);
        return list.iterator();
    }

    AbstractSQLQuery<Object, ?> query() {
        var idColumn = tableSpec().idColumn();
        var normalizedIds = ids
                .stream()
                .map(idColumn::normalize)
                .collect(toList());
        return factory().select(pathOf(bytesColumnName()))
                        .from(table())
                        .where(pathOf(idColumn).in(normalizedIds));
    }

    public static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I, R extends Message>
            extends AbstractQuery.Builder<I, R, Builder<I, R>, SelectMultipleByIds<I, R>> {

        private ImmutableList<I> ids;

        public Builder<I, R> setIds(Iterable<I> ids) {
            this.ids = ImmutableList.copyOf(ids);
            return getThis();
        }

        @Override
        protected Builder<I, R> getThis() {
            return this;
        }

        @Override
        protected SelectMultipleByIds<I, R> doBuild() {
            return new SelectMultipleByIds<>(this);
        }
    }
}
