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

import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparablePath;
import io.spine.client.CompositeFilter;
import io.spine.client.Filter;
import io.spine.server.entity.storage.Column;
import io.spine.server.storage.jdbc.query.given.QueryPredicatesTestEnv.MapToNonComparable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.querydsl.core.types.dsl.Expressions.FALSE;
import static com.querydsl.core.types.dsl.Expressions.TRUE;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static io.spine.client.CompositeFilter.CompositeOperator.ALL;
import static io.spine.client.CompositeFilter.CompositeOperator.EITHER;
import static io.spine.client.Filter.Operator.EQUAL;
import static io.spine.client.Filter.Operator.GREATER_OR_EQUAL;
import static io.spine.client.Filter.Operator.GREATER_THAN;
import static io.spine.client.Filter.Operator.LESS_OR_EQUAL;
import static io.spine.client.Filter.Operator.LESS_THAN;
import static io.spine.client.Filter.Operator.UNRECOGNIZED;
import static io.spine.client.Filters.eq;
import static io.spine.server.storage.jdbc.query.QueryPredicates.columnMatchFilter;
import static io.spine.server.storage.jdbc.query.QueryPredicates.joinPredicates;
import static io.spine.server.storage.jdbc.query.QueryPredicates.nullFilter;
import static io.spine.server.storage.jdbc.query.QueryPredicates.valueFilter;
import static io.spine.server.storage.jdbc.query.given.QueryPredicatesTestEnv.stringColumn;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"InnerClassMayBeStatic", "ClassCanBeStatic"
        /* JUnit nested classes cannot be static. */,
        "DuplicateStringLiteralInspection" /* Common test display names. */})
@DisplayName("QueryPredicates utility should")
class QueryPredicatesTest {

    private static final String COLUMN_FILTER_VALUE = "test";

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void haveUtilityConstructor() {
        assertHasPrivateParameterlessCtor(QueryPredicates.class);
    }

    @Nested
    @DisplayName("join predicates using operator")
    class JoinPredicatesUsing {

        @Test
        @DisplayName("`EITHER`")
        void either() {
            BooleanExpression left = TRUE;
            BooleanExpression right = FALSE;
            Predicate result = joinPredicates(left, right, EITHER);

            BooleanExpression expected = left.or(right);
            assertThat(result)
                    .isEqualTo(expected);
        }

        @Test
        @DisplayName("`ALL`")
        void all() {
            BooleanExpression left = TRUE;
            BooleanExpression right = FALSE;
            Predicate result = joinPredicates(left, right, ALL);

            BooleanExpression expected = left.and(right);
            assertThat(result)
                    .isEqualTo(expected);
        }
    }


    @SuppressWarnings({"CheckReturnValue", "ResultOfMethodCallIgnored"})
    // Method expected to throw exception.
    @Test
    @DisplayName("throw IAE for unsupported operator")
    void throwForUnsupportedOperator() {
        assertThrows(IllegalArgumentException.class,
                     () -> joinPredicates(TRUE,
                                          TRUE,
                                          CompositeFilter.CompositeOperator.UNRECOGNIZED));
    }

    @SuppressWarnings({"CheckReturnValue", "ResultOfMethodCallIgnored"})
    // Method expected to throw exception.
    @Test
    @DisplayName("not accept non-comparable value")
    void notAcceptNonComparable() {
        Column column = stringColumn();

        Filter filter = eq(column.name().value(), COLUMN_FILTER_VALUE);

        assertThrows(IllegalArgumentException.class,
                     () -> columnMatchFilter(column, filter, new MapToNonComparable()));
    }

    @Test
    @DisplayName("generate null filter for `EQUAL`")
    void createNullFilterForEqual() {
        ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        Predicate predicate = nullFilter(EQUAL, path);
        assertEquals(path.isNull(), predicate);
    }

    @Nested
    @DisplayName("not generate null filter for")
    class NotCreateNullFilterFor {

        @Test
        @DisplayName("`GREATER THAN`")
        void greaterThan() {
            assertThrows(IllegalArgumentException.class,
                         () -> runNullFilterCreationFor(GREATER_THAN));
        }

        @Test
        @DisplayName("`LESS THAN`")
        void lessThan() {
            assertThrows(IllegalArgumentException.class,
                         () -> runNullFilterCreationFor(LESS_THAN));
        }

        @Test
        @DisplayName("`GREATER OR EQUAL`")
        void greaterOrEqual() {
            assertThrows(IllegalArgumentException.class,
                         () -> runNullFilterCreationFor(GREATER_OR_EQUAL));
        }

        @Test
        @DisplayName("`LESS OR EQUAL`")
        void lessOrEqual() {
            assertThrows(IllegalArgumentException.class,
                         () -> runNullFilterCreationFor(LESS_OR_EQUAL));
        }

        @Test
        @DisplayName("`UNRECOGNIZED`")
        void unrecognized() {
            assertThrows(IllegalArgumentException.class,
                         () -> runNullFilterCreationFor(UNRECOGNIZED));
        }
    }

    @Nested
    @DisplayName("generate value filter for")
    class CreateValueFilterFor {

        @Test
        @DisplayName("`EQUAL`")
        void equal() {
            ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
            Predicate predicate = valueFilter(EQUAL, path, COLUMN_FILTER_VALUE);
            assertEquals(path.eq(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`GREATER THAN`")
        void greaterThan() {
            ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
            Predicate predicate = valueFilter(GREATER_THAN, path, COLUMN_FILTER_VALUE);
            assertEquals(path.gt(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`LESS THAN`")
        void lessThan() {
            ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
            Predicate predicate = valueFilter(LESS_THAN, path, COLUMN_FILTER_VALUE);
            assertEquals(path.lt(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`GREATER OR EQUAL`")
        void greaterOrEqual() {
            ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
            Predicate predicate = valueFilter(GREATER_OR_EQUAL, path, COLUMN_FILTER_VALUE);
            assertEquals(path.goe(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`LESS OR EQUAL`")
        void lessOrEqual() {
            ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
            Predicate predicate = valueFilter(LESS_OR_EQUAL, path, COLUMN_FILTER_VALUE);
            assertEquals(path.loe(COLUMN_FILTER_VALUE), predicate);
        }
    }

    @Test
    @DisplayName("not generate value filter for `UNRECOGNIZED`")
    void notCreateValueFilterForUnrecognized() {
        assertThrows(IllegalArgumentException.class,
                     () -> runValueFilterCreationFor(UNRECOGNIZED));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // Method called to throw exception.
    private static void runNullFilterCreationFor(Filter.Operator operator) {
        ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        nullFilter(operator, path);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // Method called to throw exception.
    private static void runValueFilterCreationFor(Filter.Operator operator) {
        ComparablePath<Comparable> path = comparablePath(Comparable.class, "");
        valueFilter(operator, path, COLUMN_FILTER_VALUE);
    }
}
