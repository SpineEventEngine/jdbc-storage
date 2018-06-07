/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import io.spine.client.ColumnFilter;
import io.spine.client.CompositeColumnFilter;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

import java.io.Serializable;
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
 *
 * @author Dmytro Grankin
 */
public class QueryPredicates {

    private QueryPredicates() {
        // Prevent instantiation of this utility class.
    }

    /**
     * Creates a predicate to match an {@link IdColumn ID} to one of the specified IDs.
     *
     * <p>If there are no IDs, the resulting predicate will return {@code true} always.
     *
     * @param column the {@link IdColumn} describing ID to match against
     * @param ids    the IDs to match
     * @param <I>    the type of IDs
     * @return the predicate for the IDs
     */
    public static <I> Predicate inIds(IdColumn<I> column, Collection<I> ids) {
        if (ids.isEmpty()) {
            return TRUE;
        }

        final PathBuilder<Object> id = new PathBuilder<>(Object.class, column.getColumnName());
        final Collection<Object> normalizedIds = column.normalize(ids);
        return id.in(normalizedIds);
    }

    /**
     * Obtains a predicate to match entity records by the specified parameters.
     *
     * @param parameters         the query parameters to compose the predicate
     * @param columnTypeRegistry the registry of entity column type to use
     * @return the predicate for columns
     */
    public static Predicate matchParameters(QueryParameters parameters,
                                            ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry) {
        BooleanExpression result = TRUE;
        for (CompositeQueryParameter parameter : parameters) {
            result = result.and(predicateFrom(parameter, columnTypeRegistry));
        }
        return result;
    }

    private static Predicate predicateFrom(CompositeQueryParameter parameter,
                                           ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry) {
        Predicate result = TRUE;
        for (Map.Entry<EntityColumn, ColumnFilter> columnWithFilter : parameter.getFilters()
                                                                               .entries()) {
            final Predicate predicate = columnMatchFilter(columnWithFilter.getKey(),
                                                          columnWithFilter.getValue(),
                                                          columnTypeRegistry);
            result = joinPredicates(result, predicate, parameter.getOperator());
        }
        return result;
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
    @VisibleForTesting
    static Predicate joinPredicates(Predicate left,
                                    Predicate right,
                                    CompositeColumnFilter.CompositeOperator operator) {
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

    private static Predicate columnMatchFilter(EntityColumn column, ColumnFilter filter,
                                               ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry) {
        final ColumnFilter.Operator operator = filter.getOperator();
        checkArgument(operator.getNumber() > 0, operator.name());

        final String columnName = column.getStoredName();
        final ComparablePath<Comparable> columnPath = comparablePath(Comparable.class, columnName);
        final JdbcColumnType<? super Object, ? super Object> columnType =
                columnTypeRegistry.get(column);
        final Object javaValue = toObject(filter.getValue(), column.getType());
        final Serializable persistedValue = column.toPersistedValue(javaValue);

        if (persistedValue == null) {
            return nullFilter(operator, columnPath);
        }

        final Object storedValue = columnType.convertColumnValue(persistedValue);
        final Class<?> storedValueType = storedValue.getClass();
        if (!Comparable.class.isAssignableFrom(storedValueType)) {
            final Class<?> javaValueType = javaValue.getClass();
            throw newIllegalArgumentException(
                    "Filter value of class %s is stored as non-Comparable type %s",
                    javaValueType.getCanonicalName(),
                    storedValueType.getCanonicalName());
        }
        final Comparable columnValue = (Comparable) storedValue;
        return valueFilter(operator, columnPath, columnValue);
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
    private static Predicate nullFilter(ColumnFilter.Operator operator,
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
    private static Predicate valueFilter(ColumnFilter.Operator operator,
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
