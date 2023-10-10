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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparablePath;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.query.ComparisonOperator;
import io.spine.query.LogicalOperator;
import io.spine.query.QueryPredicate;
import io.spine.query.SubjectParameter;
import io.spine.server.storage.jdbc.record.column.IdColumn;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import java.util.Collection;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.querydsl.core.types.dsl.Expressions.FALSE;
import static com.querydsl.core.types.dsl.Expressions.TRUE;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * A utility methods to work with {@linkplain Predicate predicates}.
 */
public final class QueryPredicates {

    /** Prevents instantiation of this utility class. */
    private QueryPredicates() {
    }

    /**
     * Creates a predicate to match an {@link IdColumn ID} to one of the specified IDs.
     *
     * <p>If there are no IDs, the resulting predicate will return {@code true} always.
     *
     * @param column
     *         the {@link IdColumn} describing ID to match against
     * @param ids
     *         the IDs to match
     * @param <I>
     *         the type of IDs
     * @return the predicate for the IDs
     */
    static <I> Predicate inIds(IdColumn<I> column, Collection<I> ids) {
        checkNotNull(column);
        checkNotNull(ids);

        if (ids.isEmpty()) {
            return TRUE;
        }

        var id = new PathBuilder<>(Object.class, column.columnName());
        var normalizedIds = column.normalize(ids);
        return id.in(normalizedIds);
    }

    @VisibleForTesting
    @SuppressWarnings("rawtypes")   /* To avoid the hell in generics. */
    static Predicate nullFilter(ComparisonOperator operator,
                                ComparablePath<Comparable> columnPath) {
        checkNotNull(operator);
        checkNotNull(columnPath);
        switch (operator) {
            case EQUALS:
                return columnPath.isNull();
            case GREATER_THAN:
            case LESS_THAN:
            case GREATER_OR_EQUALS:
            case LESS_OR_EQUALS:
                throw newIllegalArgumentException(
                        "Operator %s not supported for the null filter value.", operator);
            default:
                throw newIllegalArgumentException("Unexpected filter operator %s.", operator);
        }
    }

    @VisibleForTesting
    @SuppressWarnings("rawtypes")   /* To avoid the hell in generics. */
    static Predicate valueFilter(ComparablePath<Comparable> columnPath,
                                 ComparisonOperator operator,
                                 Comparable columnValue) {
        checkNotNull(columnPath);
        checkNotNull(operator);
        checkNotNull(columnValue);
        switch (operator) {
            case EQUALS:
                return columnPath.eq(columnValue);
            case GREATER_THAN:
                return columnPath.gt(columnValue);
            case LESS_THAN:
                return columnPath.lt(columnValue);
            case GREATER_OR_EQUALS:
                return columnPath.goe(columnValue);
            case LESS_OR_EQUALS:
                return columnPath.loe(columnValue);
            default:
                throw newIllegalArgumentException("Unexpected operator %s.", operator);
        }
    }

    @SuppressWarnings("rawtypes")   /* To avoid the hell in generics. */
    private static Predicate
    matchParameter(SubjectParameter<?, ?, ?> parameter, JdbcColumnMapping columnMapping) {
        var column = parameter.column();
        var value = parameter.value();
        var operator = parameter.operator();

        var columnName = column.name()
                               .value();
        var columnPath = comparablePath(Comparable.class, columnName);

        var typeMapping = columnMapping.of(column.type());
        var convertedValue = typeMapping.applyTo(value);
        if (convertedValue == null) {
            return nullFilter(operator, columnPath);
        }
        checkIsComparable(convertedValue, value);
        var columnValue = (Comparable) convertedValue;
        var result = valueFilter(columnPath, operator, columnValue);
        return result;
    }

    /**
     * Obtains a QueryDSL-specific predicate to match the records
     * by the specified {@link QueryPredicate}.
     *
     * @param predicate
     *         the query predicate to use for matching
     * @param columnMapping
     *         the entity column mapping
     * @param <R>
     *         the type of the queried records
     * @return the QueryDSL-specific predicate for the records
     */
    static <R extends Message> Predicate
    matchPredicate(QueryPredicate<R> predicate, JdbcColumnMapping columnMapping) {
        checkNotNull(predicate);
        checkNotNull(columnMapping);

        AssemblePredicate assembler;
        BooleanExpression current;
        var operator = predicate.operator();
        if (operator == LogicalOperator.AND) {
            current = TRUE;
            assembler = BooleanExpression::and;
        } else {
            current = FALSE;
            assembler = BooleanExpression::or;
        }

        current = includeSimpleParams(predicate.allParams(), current, assembler, columnMapping);
        current = includeChildPredicates(predicate.children(), current, assembler, columnMapping);

        return current;
    }

    private static BooleanExpression
    includeSimpleParams(ImmutableList<SubjectParameter<?, ?, ?>> params,
                        BooleanExpression initialValue,
                        AssemblePredicate assembler,
                        JdbcColumnMapping columnMapping) {
        var currentValue = initialValue;
        for (var parameter : params) {
            var fromParam = matchParameter(parameter, columnMapping);
            currentValue = assembler.apply(currentValue, fromParam);
        }
        return currentValue;
    }

    private static <R extends Message> BooleanExpression
    includeChildPredicates(ImmutableList<QueryPredicate<R>> children,
                           BooleanExpression initialValue,
                           AssemblePredicate assembler,
                           JdbcColumnMapping columnMapping) {
        var currentValue = initialValue;
        for (var childPredicate : children) {
            var fromChild = matchPredicate(childPredicate, columnMapping);
            currentValue = assembler.apply(currentValue, fromChild);
        }
        return currentValue;
    }

    /**
     * Checks that {@code storedValue} implements {@link Comparable}.
     *
     * <p>{@code javaValue} is passed for logging purposes only.
     */
    private static void checkIsComparable(Object storedValue, Object javaValue) {
        var storedType = storedValue.getClass();
        if (!Comparable.class.isAssignableFrom(storedType)) {
            var javaType = javaValue.getClass();
            throw newIllegalArgumentException(
                    "Received filter value of class %s which has non-Comparable storage type %s",
                    javaType.getCanonicalName(),
                    storedType.getCanonicalName());
        }
    }

    @FunctionalInterface
    private interface AssemblePredicate
            extends BiFunction<BooleanExpression, Predicate, BooleanExpression> {
    }
}
