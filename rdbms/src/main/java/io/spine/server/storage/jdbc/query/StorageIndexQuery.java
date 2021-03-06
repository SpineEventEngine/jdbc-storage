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

import java.sql.ResultSet;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idReader;

/**
 * A query for all the IDs in a certain table.
 *
 * @param <I>
 *         the type of IDs
 */
class StorageIndexQuery<I> extends AbstractQuery implements SelectQuery<Iterator<I>> {

    private final IdColumn<I> idColumn;

    private StorageIndexQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
    }

    @Override
    public Iterator<I> execute() {
        ResultSet resultSet = factory().select(pathOf(idColumn))
                                       .distinct()
                                       .from(table())
                                       .getResults();
        Class<I> columnType = idColumn.javaType();
        ColumnReader<I> idColumnReader = idReader(idColumn.columnName(), columnType);
        DbIterator<I> result = DbIterator.over(resultSet, idColumnReader);
        return result;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>, StorageIndexQuery<I>> {

        private IdColumn<I> idColumn;

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return getThis();
        }

        @Override
        protected StorageIndexQuery<I> doBuild() {
            return new StorageIndexQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
