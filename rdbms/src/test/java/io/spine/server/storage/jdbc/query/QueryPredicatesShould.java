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

import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparablePath;
import io.spine.client.ColumnFilter;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import org.junit.Test;

import static com.querydsl.core.types.dsl.Expressions.FALSE;
import static com.querydsl.core.types.dsl.Expressions.TRUE;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static io.spine.client.ColumnFilter.Operator.EQUAL;
import static io.spine.client.ColumnFilter.Operator.GREATER_OR_EQUAL;
import static io.spine.client.ColumnFilter.Operator.GREATER_THAN;
import static io.spine.client.ColumnFilter.Operator.LESS_OR_EQUAL;
import static io.spine.client.ColumnFilter.Operator.LESS_THAN;
import static io.spine.client.ColumnFilters.eq;
import static io.spine.client.ColumnFilters.gt;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.ALL;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.EITHER;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.UNRECOGNIZED;
import static io.spine.server.storage.jdbc.query.QueryPredicates.columnMatchFilter;
import static io.spine.server.storage.jdbc.query.QueryPredicates.joinPredicates;
import static io.spine.server.storage.jdbc.query.QueryPredicates.nullFilter;
import static io.spine.server.storage.jdbc.query.QueryPredicates.valueFilter;
import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Grankin
 */
public class QueryPredicatesShould {

    private static final String COLUMN_FILTER_VALUE = "test";

    @Test
    public void have_the_private_utility_ctor() {
        assertHasPrivateParameterlessCtor(QueryPredicates.class);
    }

    @Test
    public void join_predicates_using_either_operator() {
        final BooleanExpression left = TRUE;
        final BooleanExpression right = FALSE;
        final Predicate result = joinPredicates(left, right, EITHER);
        assertEquals(left.or(right), result);
    }

    @Test
    public void join_predicates_using_all_operator() {
        final BooleanExpression left = TRUE;
        final BooleanExpression right = FALSE;
        final Predicate result = joinPredicates(left, right, ALL);
        assertEquals(left.and(right), result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throw_exception_for_unsupported_operator() {
        joinPredicates(TRUE, TRUE, UNRECOGNIZED);
    }

    @Test
    public void create_equal_predicate_for_null_value() {
        final EntityColumn column = stringColumnMock();
        when(column.toPersistedValue(any())).thenReturn(null);

        final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> registry
                = JdbcTypeRegistryFactory.defaultInstance();

        final ColumnFilter filter = eq(column.getStoredName(), COLUMN_FILTER_VALUE);
        final Predicate predicate = columnMatchFilter(column, filter, registry);

        final ComparablePath<Comparable> columnPath = comparablePath(Comparable.class,
                                                                     column.getStoredName());
        final BooleanExpression isNullPredicate = columnPath.isNull();
        assertEquals(isNullPredicate, predicate);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // Method expected to throw exception.
    @Test(expected = IllegalArgumentException.class)
    public void not_create_ordering_predicate_for_null_value() {
        final EntityColumn column = stringColumnMock();
        when(column.toPersistedValue(any())).thenReturn(null);

        final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> registry
                = JdbcTypeRegistryFactory.defaultInstance();

        final ColumnFilter filter = gt(column.getStoredName(), COLUMN_FILTER_VALUE);
        columnMatchFilter(column, filter, registry);
    }

    @SuppressWarnings({"unchecked" /* Using raw types for mocks. */,
                       "ResultOfMethodCallIgnored" /* Method expected to throw exception. */})
    @Test(expected = IllegalArgumentException.class)
    public void not_accept_non_comparable_value() {
        final EntityColumn column = stringColumnMock();
        when(column.toPersistedValue(any())).thenReturn("test value");

        final JdbcColumnType type = mock(JdbcColumnType.class);
        when(type.convertColumnValue(any())).thenReturn(new Object());

        final ColumnTypeRegistry registry = ColumnTypeRegistry.newBuilder()
                                                              .put(String.class, type)
                                                              .build();

        final ColumnFilter filter = eq(column.getStoredName(), COLUMN_FILTER_VALUE);
        columnMatchFilter(column, filter, registry);
    }

    @Test
    public void generate_null_filter_for_EQUAL() {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        final Predicate predicate = nullFilter(EQUAL, path);
        assertEquals(path.isNull(), predicate);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_generate_null_filter_for_GREATER_THAN() {
        runNullFilterCreationFor(GREATER_THAN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_generate_null_filter_for_LESS_THAN() {
        runNullFilterCreationFor(LESS_THAN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_generate_null_filter_for_GREATER_OR_EQUAL() {
        runNullFilterCreationFor(GREATER_OR_EQUAL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_generate_null_filter_for_LESS_OR_EQUAL() {
        runNullFilterCreationFor(LESS_OR_EQUAL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_generate_null_filter_for_UNRECOGNIZED() {
        runNullFilterCreationFor(ColumnFilter.Operator.UNRECOGNIZED);
    }

    @Test
    public void generate_value_filter_for_EQUAL() {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        final Predicate predicate = valueFilter(EQUAL, path, COLUMN_FILTER_VALUE);
        assertEquals(path.eq(COLUMN_FILTER_VALUE), predicate);
    }

    @Test
    public void generate_value_filter_for_GREATER_THAN() {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        final Predicate predicate = valueFilter(GREATER_THAN, path, COLUMN_FILTER_VALUE);
        assertEquals(path.gt(COLUMN_FILTER_VALUE), predicate);
    }

    @Test
    public void generate_value_filter_for_LESS_THAN() {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        final Predicate predicate = valueFilter(LESS_THAN, path, COLUMN_FILTER_VALUE);
        assertEquals(path.lt(COLUMN_FILTER_VALUE), predicate);
    }

    @Test
    public void generate_value_filter_for_GREATER_OR_EQUAL() {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        final Predicate predicate = valueFilter(GREATER_OR_EQUAL, path, COLUMN_FILTER_VALUE);
        assertEquals(path.goe(COLUMN_FILTER_VALUE), predicate);
    }

    @Test
    public void generate_value_filter_for_LESS_OR_EQUAL() {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        final Predicate predicate = valueFilter(LESS_OR_EQUAL, path, COLUMN_FILTER_VALUE);
        assertEquals(path.loe(COLUMN_FILTER_VALUE), predicate);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_generate_value_filter_for_UNRECOGNIZED() {
        runValueFilterCreationFor(ColumnFilter.Operator.UNRECOGNIZED);
    }

    private static EntityColumn stringColumnMock() {
        final EntityColumn column = mock(EntityColumn.class);
        when(column.getStoredName()).thenReturn("test column");
        when(column.getType()).thenReturn(String.class);
        when(column.getPersistedType()).thenReturn(String.class);
        return column;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // Method called to throw exception.
    private static void runNullFilterCreationFor(ColumnFilter.Operator operator) {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        nullFilter(operator, path);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // Method called to throw exception.
    private static void runValueFilterCreationFor(ColumnFilter.Operator operator) {
        final ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        valueFilter(operator, path, COLUMN_FILTER_VALUE);
    }
}
