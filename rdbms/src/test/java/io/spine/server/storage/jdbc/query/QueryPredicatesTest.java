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

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import com.querydsl.core.types.dsl.ComparablePath;
import io.spine.query.ComparisonOperator;
import io.spine.query.QueryPredicate;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.record.column.IdColumn;
import io.spine.server.storage.jdbc.type.DefaultJdbcColumnMapping;
import io.spine.test.storage.StgProject;
import io.spine.testing.UtilityClassTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static io.spine.query.ComparisonOperator.EQUALS;
import static io.spine.query.ComparisonOperator.GREATER_OR_EQUALS;
import static io.spine.query.ComparisonOperator.GREATER_THAN;
import static io.spine.query.ComparisonOperator.LESS_OR_EQUALS;
import static io.spine.query.ComparisonOperator.LESS_THAN;
import static io.spine.server.storage.jdbc.query.QueryPredicates.nullFilter;
import static io.spine.server.storage.jdbc.query.QueryPredicates.valueFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("QueryPredicates utility should")
@SuppressWarnings({"InnerClassMayBeStatic", "ClassCanBeStatic"
        /* JUnit nested classes cannot be static. */,
        "DuplicateStringLiteralInspection" /* Common test display names. */})
class QueryPredicatesTest extends UtilityClassTest<QueryPredicates> {

    private static final String COLUMN_FILTER_VALUE = "test";

    QueryPredicatesTest() {
        super(QueryPredicates.class, Visibility.PACKAGE);
    }

    @Override
    protected void configure(NullPointerTester tester) {
        super.configure(tester);
        var mapping = new DefaultJdbcColumnMapping();
        var idColumn = IdColumn.of(new TableColumn("sample_id", String.class, mapping));
        var predicate = StgProject.query()
                                  .build()
                                  .subject()
                                  .predicate();
        tester.setDefault(ComparablePath.class, comparablePath(String.class, ""))
              .setDefault(IdColumn.class, idColumn)
              .setDefault(QueryPredicate.class, predicate);
    }

    @Test
    @DisplayName("generate `null` filter for `EQUAL`")
    void createNullFilterForEqual() {
        var path = comparablePath(Comparable.class, "");
        var predicate = nullFilter(EQUALS, path);
        assertEquals(path.isNull(), predicate);
    }

    @Nested
    @DisplayName("not generate `null` filter for")
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
                         () -> runNullFilterCreationFor(GREATER_OR_EQUALS));
        }

        @Test
        @DisplayName("`LESS OR EQUAL`")
        void lessOrEqual() {
            assertThrows(IllegalArgumentException.class,
                         () -> runNullFilterCreationFor(LESS_OR_EQUALS));
        }
    }

    @Nested
    @DisplayName("generate value filter for")
    class CreateValueFilterFor {

        @Test
        @DisplayName("`EQUAL`")
        void equal() {
            var path = comparablePath(Comparable.class, "");
            var predicate = valueFilter(path, EQUALS, COLUMN_FILTER_VALUE);
            assertEquals(path.eq(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`GREATER THAN`")
        void greaterThan() {
            var path = comparablePath(Comparable.class, "");
            var predicate = valueFilter(path, GREATER_THAN, COLUMN_FILTER_VALUE);
            assertEquals(path.gt(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`LESS THAN`")
        void lessThan() {
            var path = comparablePath(Comparable.class, "");
            var predicate = valueFilter(path, LESS_THAN, COLUMN_FILTER_VALUE);
            assertEquals(path.lt(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`GREATER OR EQUAL`")
        void greaterOrEqual() {
            var path = comparablePath(Comparable.class, "");
            var predicate = valueFilter(path, GREATER_OR_EQUALS, COLUMN_FILTER_VALUE);
            assertEquals(path.goe(COLUMN_FILTER_VALUE), predicate);
        }

        @Test
        @DisplayName("`LESS OR EQUAL`")
        void lessOrEqual() {
            var path = comparablePath(Comparable.class, "");
            var predicate = valueFilter(path, LESS_OR_EQUALS, COLUMN_FILTER_VALUE);
            assertEquals(path.loe(COLUMN_FILTER_VALUE), predicate);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // Method called to throw exception.
    private static void runNullFilterCreationFor(ComparisonOperator operator) {
        var path = comparablePath(Comparable.class, "");
        nullFilter(operator, path);
    }
}
