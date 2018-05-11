/*
 * Copyright 2017, TeamDev. All rights reserved.
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
import org.junit.Test;

import static com.querydsl.core.types.dsl.Expressions.FALSE;
import static com.querydsl.core.types.dsl.Expressions.TRUE;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.ALL;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.EITHER;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.UNRECOGNIZED;
import static io.spine.server.storage.jdbc.query.QueryPredicates.joinPredicates;
import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Grankin
 */
public class QueryPredicatesShould {

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
}
