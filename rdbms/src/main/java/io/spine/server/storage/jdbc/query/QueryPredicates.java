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
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparablePath;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.client.CompositeFilter;
import io.spine.client.Filter;
import io.spine.client.Filter.Operator;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnTypeMapping;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.querydsl.core.types.ExpressionUtils.and;
import static com.querydsl.core.types.ExpressionUtils.or;
import static com.querydsl.core.types.dsl.Expressions.TRUE;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static io.spine.protobuf.TypeConverter.toObject;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * A utility methods to work with {@linkplain Predicate predicates}.
 */
public class QueryPredicates {

    /** Prevent instantiation of this utility class. */
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
    public static <I> Predicate inIds(IdColumn<I> column, Collection<I> ids) {
        if (ids.isEmpty()) {
            return TRUE;
        }

        PathBuilder<Object> id = new PathBuilder<>(Object.class, column.columnName());
        Collection<Object> normalizedIds = column.normalize(ids);
        return id.in(normalizedIds);
    }

    /**
     * Obtains a predicate to match entity records by the specified parameters.
     *
     * @param parameters
     *         the query parameters to compose the predicate
     * @param columnMapping
     *         the entity column mapping
     * @return the predicate for columns
     */
    public static Predicate
    matchParameters(QueryParameters parameters, JdbcColumnMapping<?> columnMapping) {
        BooleanExpression result = TRUE;
        for (CompositeQueryParameter parameter : parameters) {
            result = result.and(predicateFrom(parameter, columnMapping));
        }
        return result;
    }

    private static Predicate
    predicateFrom(CompositeQueryParameter parameter, JdbcColumnMapping<?> columnMapping) {
        Predicate result = TRUE;
        for (Map.Entry<Column, Filter> columnWithFilter : parameter.filters()
                                                                   .entries()) {
            Predicate predicate = columnMatchFilter(columnWithFilter.getKey(),
                                                    columnWithFilter.getValue(),
                                                    columnMapping);
            result = joinPredicates(result, predicate, parameter.operator());
        }
        return result;
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
    @VisibleForTesting
    static Predicate joinPredicates(Predicate left,
                                    Predicate right,
                                    CompositeFilter.CompositeOperator operator) {
        checkArgument(operator.getNumber() > 0, operator.name());
        switch (operator) {
            case EITHER:
                return or(left, right);
            case ALL:
                return and(left, right);
            default:
                throw newIllegalArgumentException("Unexpected composite operator %s.",
                                                  operator);
        }
    }

    @VisibleForTesting
    static Predicate
    columnMatchFilter(Column column, Filter filter, JdbcColumnMapping<?> columnMapping) {
        Operator operator = filter.getOperator();
        checkArgument(operator.getNumber() > 0, operator.name());

        String columnName = column.name()
                                  .value();
        ComparablePath<Comparable> columnPath = comparablePath(Comparable.class, columnName);
        Class<?> type = column.type();
        Object javaValue = toObject(filter.getValue(), type);
        ColumnTypeMapping<?, ?> mapping =
                columnMapping.of(javaValue.getClass());
        Object valueForStoring = mapping.applyTo(javaValue);
        if (valueForStoring == null) {
            return nullFilter(operator, columnPath);
        }
        checkIsComparable(valueForStoring, javaValue);
        Comparable columnValue = (Comparable) valueForStoring;
        return valueFilter(operator, columnPath, columnValue);
    }

    /**
     * Checks that {@code storedValue} implements {@link Comparable}.
     *
     * <p>{@code javaValue} is passed for logging purposes only.
     */
    private static void checkIsComparable(Object storedValue, Object javaValue) {
        Class<?> storedType = storedValue.getClass();
        if (!Comparable.class.isAssignableFrom(storedType)) {
            Class<?> javaType = javaValue.getClass();
            throw newIllegalArgumentException(
                    "Received filter value of class %s which has non-Comparable storage type %s",
                    javaType.getCanonicalName(),
                    storedType.getCanonicalName());
        }
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
    @VisibleForTesting
    static Predicate nullFilter(Operator operator,
                                ComparablePath<Comparable> columnPath) {
        switch (operator) {
            case EQUAL:
                return columnPath.isNull();
            case GREATER_THAN:
            case LESS_THAN:
            case GREATER_OR_EQUAL:
            case LESS_OR_EQUAL:
                throw newIllegalArgumentException(
                        "Operator %s not supported for the null filter value.", operator);
            default:
                throw newIllegalArgumentException("Unexpected filter operator %s.", operator);
        }
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
    @VisibleForTesting
    static Predicate valueFilter(Operator operator,
                                 ComparablePath<Comparable> columnPath,
                                 Comparable columnValue) {
        switch (operator) {
            case EQUAL:
                return columnPath.eq(columnValue);
            case GREATER_THAN:
                return columnPath.gt(columnValue);
            case LESS_THAN:
                return columnPath.lt(columnValue);
            case GREATER_OR_EQUAL:
                return columnPath.goe(columnValue);
            case LESS_OR_EQUAL:
                return columnPath.loe(columnValue);
            default:
                throw newIllegalArgumentException("Unexpected operator %s.", operator);
        }
    }
}
