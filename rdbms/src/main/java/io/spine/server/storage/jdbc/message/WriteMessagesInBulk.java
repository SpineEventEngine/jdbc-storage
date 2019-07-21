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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.querydsl.core.dml.StoreClause;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.server.storage.jdbc.message.MessageTable.Column;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.IdColumn;

import java.util.Map;

abstract class WriteMessagesInBulk<I, M extends Message, C extends StoreClause<C>>
        extends AbstractQuery
        implements WriteMessageQuery<I, M> {

    private final Map<I, M> records;
    private final ImmutableList<? extends Column<M>> columns;
    private final IdColumn<I> idColumn;

    WriteMessagesInBulk(Builder<I, M, ? extends Builder, ? extends WriteMessagesInBulk> builder) {
        super(builder);
        this.records = builder.records;
        this.idColumn = builder.idColumn;
        this.columns = builder.columns;
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        C query = clause();
        records.forEach((id, record) -> addToBatch(query, id, record));
        return 0;
    }

    protected abstract C clause();

    private void addToBatch(C query, I id, M record) {
        setIdClause(query, id, record);
        setColumnValues(query, record);
        addBatch(query);
    }

    protected abstract void setIdClause(C query, I id, M record);

    protected abstract void addBatch(C query);

    @Override
    public Iterable<? extends Column<M>> columns() {
        return columns;
    }

    @Override
    public IdColumn<I> idColumn() {
        return idColumn;
    }

    @Override
    public PathBuilder<Object> pathOf(Column<M> column) {
        return super.pathOf(column);
    }

    abstract static class Builder<I,
                                  M extends Message,
                                  B extends Builder<I, M, B, Q>,
                                  Q extends WriteMessagesInBulk>
            extends AbstractQuery.Builder<B, Q> {

        private Map<I, M> records;
        private ImmutableList<? extends Column<M>> columns;
        private IdColumn<I> idColumn;

        B setRecords(Map<I, M> records) {
            this.records = ImmutableMap.copyOf(records);
            return getThis();
        }

        B setColumns(Iterable<? extends Column<M>> columns) {
            this.columns = ImmutableList.copyOf(columns);
            return getThis();
        }

        B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
