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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.sql.dml.SQLDeleteClause;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.Map;

import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.AGGREGATE;
import static io.spine.server.storage.jdbc.query.Serializer.serialize;

/**
 * A query that deletes the specified {@link AggregateEventRecord aggregate event records} from the
 * table.
 *
 * <p>As records in table can share a common {@code Aggregate} ID, and the record state can in
 * theory be equal to other record's state, the deletion is done by the unique
 * "{@code Aggregate}_ID-to-record_state" combination.
 *
 * @param <I>
 *         the type of {@code Aggregate} IDs
 */
final class DeleteAggregateRecordsQuery<I> extends AbstractQuery implements WriteQuery {

    private final IdColumn<I> idColumn;
    private final Multimap<I, AggregateEventRecord> records;

    private DeleteAggregateRecordsQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.records = builder.records;
    }

    @Override
    public long execute() {
        SQLDeleteClause query = factory().delete(table())
                                         .where(buildPredicate());
        return query.execute();
    }

    @SuppressWarnings("CheckReturnValue") // Calling the builder method.
    private Predicate buildPredicate() {
        BooleanBuilder predicateBuilder = new BooleanBuilder();
        records.entries()
               .stream()
               .map(this::toPredicate)
               .forEach(predicateBuilder::or);
        return predicateBuilder.getValue();
    }

    private Predicate toPredicate(Map.Entry<I, AggregateEventRecord> entry) {
        I id = entry.getKey();
        AggregateEventRecord record = entry.getValue();
        Predicate predicate = idMatches(id).and(recordMatches(record));
        return predicate;
    }

    private BooleanExpression idMatches(I id) {
        Object normalizedId = idColumn.normalize(id);
        BooleanExpression predicate = pathOf(idColumn.columnName()).eq(normalizedId);
        return predicate;
    }

    private BooleanExpression recordMatches(AggregateEventRecord record) {
        byte[] serializedRecord = serialize(record);
        BooleanExpression predicate = pathOf(AGGREGATE).eq(serializedRecord);
        return predicate;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I>
            extends AbstractQuery.Builder<Builder<I>, DeleteAggregateRecordsQuery<I>> {

        private IdColumn<I> idColumn;
        private Multimap<I, AggregateEventRecord> records;

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        Builder<I> setRecords(Multimap<I, AggregateEventRecord> records) {
            this.records = HashMultimap.create(records);
            return getThis();
        }

        @Override
        protected DeleteAggregateRecordsQuery<I> doBuild() {
            return new DeleteAggregateRecordsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
