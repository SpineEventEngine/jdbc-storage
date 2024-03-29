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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.dml.SQLInsertClause;
import com.querydsl.sql.dml.SQLUpdateClause;
import io.spine.query.ColumnName;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.RecordTable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract base for queries that modify a single record to a {@link RecordTable}.
 *
 * @param <I>
 *         the record ID type
 * @param <R>
 *         the record type
 */
public abstract class WriteOneQuery<I, R extends Message> extends WriteQuery<I, R> {

    private final JdbcRecord<I, R> record;

    WriteOneQuery(Builder<I, R, ? extends Builder<I, R, ?, ?>, ? extends WriteOneQuery<I, R>> b) {
        super(b);
        this.record = checkNotNull(b.record);
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        var query = clause();
        setColumnValues(query, record);
        return query.execute();
    }

    /**
     * Obtains an SQL clause to use, basically {@code INSERT} or {@code UPDATE}.
     */
    protected abstract StoreClause<?> clause();

    /**
     * Returns the record to write.
     */
    protected final JdbcRecord<I, R> record() {
        return record;
    }

    protected SQLInsertClause insertWithId() {
        var query = factory().insert(table())
                             .set(idPath(), normalizedId());
        return query;
    }

    protected Object normalizedId() {
        return idColumn().normalize(recordId());
    }

    protected Predicate idEquals() {
        return idPath().eq(normalizedId());
    }

    protected SQLUpdateClause updateById() {
        var query = factory().update(table()).where(idEquals());
        return query;
    }

    private I recordId() {
        return record().id();
    }

    @Override
    public PathBuilder<Object> pathOf(TableColumn column) {
        return super.pathOf(column);
    }

    @Override
    public PathBuilder<Object> pathOf(ColumnName name) {
        return super.pathOf(name);
    }

    @SuppressWarnings("ClassNameSameAsAncestorName" /* For simplicity. */)
    public abstract static class Builder<I,
                                  R extends Message,
                                  B extends Builder<I, R, B, Q>,
                                  Q extends WriteOneQuery<I, R>>
            extends AbstractQuery.Builder<I, R, B, Q> {

        private JdbcRecord<I, R> record;

        public B setRecord(JdbcRecord<I, R> record) {
            this.record = record;
            return getThis();
        }
    }
}
