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

package io.spine.server.storage.jdbc;

import org.junit.Test;

import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Dashenkov
 */
public class SqlShould {

    @Test
    public void have_private_constructor() {
        assertHasPrivateParameterlessCtor(Sql.class);
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    @Test
    public void provide_valid_sql_tokens() {
        final String createTableExpected = "CREATE TABLE";
        final String createTableActual = Sql.Query.CREATE_TABLE.toString()
                                                               .trim();
        assertEquals(createTableExpected, createTableActual);

        final String countExpected = "COUNT";
        final String countActual = Sql.Function.COUNT.toString()
                                                     .trim();
        assertEquals(countExpected, countActual);
    }

    @Test
    public void provide_tokens_wrapped_into_whitespace() {
        final String sumExpected = " SUM ";
        final String sumActual = Sql.Function.SUM.toString();
        assertEquals(sumExpected, sumActual);

        final String primaryKeyExpected = " PRIMARY KEY ";
        final String primaryKeyActual = Sql.Query.PRIMARY_KEY.toString();
        assertEquals(primaryKeyExpected, primaryKeyActual);

        final String commaExpected = " , ";
        final String commaActual = Sql.BuildingBlock.COMMA.toString();
        assertEquals(commaExpected, commaActual);
    }
}
