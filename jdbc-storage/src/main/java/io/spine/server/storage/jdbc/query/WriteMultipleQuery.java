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

package io.spine.server.storage.jdbc.query;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.RecordTable;

import static java.util.Objects.requireNonNull;

/**
 * An abstract base for queries that write multiple messages
 * to a {@link RecordTable} in a batch.
 *
 * @param <I>
 *         the record ID type
 * @param <R>
 *         the record type
 * @param <C>
 *         the type of SQL clause
 */
abstract class WriteMultipleQuery<I, R extends Message, C extends StoreClause<C>>
        extends WriteQuery<I, R> {

    private final ImmutableList<JdbcRecord<I, R>> records;

    WriteMultipleQuery(Builder<I, R, ? extends Builder<I, R, ?, ?>,
            ? extends WriteMultipleQuery<I, R, ?>> builder) {
        super(builder);
        this.records = builder.records;
    }

    @Override
    @CanIgnoreReturnValue
    public long execute() {
        if (records.isEmpty()) {
            return 0;
        }
        var query = clause();
        records.forEach(record -> addToBatch(query, record));
        return query.execute();
    }

    /**
     * Obtains an SQL clause to use, basically {@code INSERT} or {@code UPDATE}.
     */
    protected abstract C clause();

    private void addToBatch(C query, JdbcRecord<I, R> recordWithCols) {
        var id = recordWithCols.id();
        setIdClause(query, id);
        setColumnValues(query, recordWithCols);
        addBatch(query);
    }

    /**
     * Sets the ID clause for the given {@code record}.
     */
    protected abstract void setIdClause(C query, I id);

    /**
     * Adds current state of the {@code query} to the processing batch.
     */
    protected abstract void addBatch(C query);

    @SuppressWarnings("ClassNameSameAsAncestorName" /* For simplicity. */)
    public abstract static class Builder<I,
                                         R extends Message,
                                         B extends Builder<I, R, B, Q>,
                                         Q extends WriteMultipleQuery<I, R, ?>>
            extends AbstractQuery.Builder<I, R, B, Q> {

        private ImmutableList<JdbcRecord<I, R>> records;

        public B setRecords(ImmutableList<JdbcRecord<I, R>> records) {
            this.records = requireNonNull(records);
            return getThis();
        }
    }
}
