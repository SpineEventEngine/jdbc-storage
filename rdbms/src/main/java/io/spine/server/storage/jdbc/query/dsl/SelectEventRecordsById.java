/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.query.dsl;

import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DbIterator;
import io.spine.server.storage.jdbc.EventCountTable.Column;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.MessageDbIterator;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;

import java.sql.ResultSet;

import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.aggregate;
import static io.spine.type.TypeUrl.of;

/**
 * Query that selects {@linkplain AggregateEventRecord event records} by an aggregate ID.
 *
 * <p>Resulting records is ordered by version descending. If the version is the same for
 * several records, they will be ordered by creation time descending.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class SelectEventRecordsById<I> extends AbstractQuery
        implements SelectByIdQuery<I, DbIterator<AggregateEventRecord>>{

    private final IdColumn<I> idColumn;
    private final I id;
    private final int batchSize;

    private SelectEventRecordsById(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
        this.batchSize = builder.batchSize;
    }

    @Override
    public DbIterator<AggregateEventRecord> execute() {
        final PathBuilder<Object> path = new PathBuilder<>(Object.class, getTableName());
        final BooleanExpression predicate = path.get(Column.id.name())
                                                .eq(idColumn.normalize(id));
        final ResultSet resultSet = factory().selectFrom(getTable())
                                             .where(predicate)
                                             .getResults();
        return new MessageDbIterator<>(resultSet,
                                       aggregate.toString(),
                                       of(AggregateEventRecord.class));
    }

    @Override
    public IdColumn<I> getIdColumn() {
        return idColumn;
    }

    @Override
    public I getId() {
        return id;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>,
                                                          SelectEventRecordsById<I>> {

        private IdColumn<I> idColumn;
        private I id;
        private int batchSize;

        Builder<I> setId(I id) {
            this.id = id;
            return getThis();
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        Builder<I> setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return getThis();
        }

        @Override
        public SelectEventRecordsById<I> build() {
            return new SelectEventRecordsById<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
