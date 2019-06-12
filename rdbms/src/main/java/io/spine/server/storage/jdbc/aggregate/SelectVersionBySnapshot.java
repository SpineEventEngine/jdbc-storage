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

import com.google.protobuf.Timestamp;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparablePath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.SimpleExpression;
import com.querydsl.sql.AbstractSQLQuery;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.ColumnReader;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.DbIterator.DoubleColumnRecord;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.querydsl.core.types.Order.DESC;
import static com.querydsl.sql.SQLExpressions.select;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.ID;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.KIND;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.TIMESTAMP_NANOS;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.VERSION;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idReader;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.intReader;

/**
 * Selects the version which corresponds to the specified {@code snapshotIndex}.
 *
 * <p>In case the {@link Timestamp date} is specified, selects the minimum version which is both
 * newer than the {@code date} and corresponds to a snapshot older or equal to
 * the one at {@code snapshotIndex}.
 *
 * @param <I>
 *         the type of the record IDs
 */
final class SelectVersionBySnapshot<I>
        extends AbstractQuery
        implements SelectQuery<DbIterator<DoubleColumnRecord<I, Integer>>> {

    private final IdColumn<I> idColumn;
    private final int snapshotIndex;
    private final @Nullable Timestamp date;

    private SelectVersionBySnapshot(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.snapshotIndex = builder.snapshotIndex;
        this.date = builder.date;
    }

    /**
     * Runs a query which finds the minimum version between those newer or corresponding to
     * the {@code snapshotIndex} and those newer or corresponding to the {@code date}.
     *
     * <p>The result are given on the per-aggregate-ID basis.
     */
    @Override
    public DbIterator<DoubleColumnRecord<I, Integer>> execute() {
        PathBuilder<Object> id = aliasedPathOf(ID);
        ComparablePath<Integer> version = aliasedComparablePathOf(VERSION, Integer.class);
        String versionAlias = "version";
        SimpleExpression<Integer> versionColumn = SQLExpressions.min(version)
                                                                .as(versionAlias);
        AbstractSQLQuery<Tuple, ?> query = factory()
                .select(id, versionColumn)
                .from(tableAlias())
                .groupBy(id)
                .where(versionPredicate());

        ResultSet resultSet = query.getResults();
        ColumnReader<I> idReader = idReader(idColumn.columnName(), idColumn.javaType());
        ColumnReader<Integer> versionReader = intReader(versionAlias);
        DbIterator<DoubleColumnRecord<I, Integer>> result =
                DbIterator.over(resultSet, idReader, versionReader);
        return result;
    }

    private BooleanExpression versionPredicate() {
        BooleanExpression result = aliasedPathOf(VERSION).in(newerThanSnapshot());
        if (date != null) {
            return result.or(aliasedPathOf(VERSION).in(newerThanDate()));
        }
        return result;
    }

    private SQLQuery<Object> newerThanSnapshot() {
        OrderSpecifier<Comparable> byVersion = orderBy(VERSION, DESC);
        int snapshotCount = snapshotIndex + 1;
        SQLQuery<Object> query = select(pathOf(VERSION))
                .from(table())
                .where(pathOf(ID).eq(aliasedPathOf(ID)))
                .where(pathOf(KIND.name(), String.class).eq("SNAPSHOT"))
                .orderBy(byVersion)
                .limit(snapshotCount);
        return query;
    }

    @SuppressWarnings("ConstantConditions") // Checked logically.
    private SQLQuery<Object> newerThanDate() {
        long seconds = date.getSeconds();
        int nanos = date.getNanos();

        BooleanExpression moreSeconds = comparablePathOf(TIMESTAMP, Long.class).gt(seconds);
        BooleanExpression sameSeconds = comparablePathOf(TIMESTAMP, Long.class).eq(seconds);
        BooleanExpression moreOrSameNanos =
                comparablePathOf(TIMESTAMP_NANOS, Integer.class).goe(nanos);
        Predicate isNewerOrSame = moreSeconds.or(sameSeconds.and(moreOrSameNanos));

        SQLQuery<Object> query = select(pathOf(VERSION))
                .from(table())
                .where(pathOf(ID).eq(aliasedPathOf(ID)))
                .where(isNewerOrSame);
        return query;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>, SelectVersionBySnapshot<I>> {

        private IdColumn<I> idColumn;
        private int snapshotIndex;
        private @Nullable Timestamp date;

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return getThis();
        }

        Builder<I> setSnapshotIndex(int snapshotIndex) {
            this.snapshotIndex = snapshotIndex;
            return getThis();
        }

        Builder<I> setDate(@Nullable Timestamp date) {
            this.date = date;
            return getThis();
        }

        @Override
        protected SelectVersionBySnapshot<I> doBuild() {
            return new SelectVersionBySnapshot<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
