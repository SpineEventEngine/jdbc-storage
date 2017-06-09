/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.test.Tests.hasPrivateParameterlessCtor;

/**
 * @author Dmytro Dashenkov
 */
public class SqlShould {

    @Test
    public void have_private_constructor() {
        assertTrue(hasPrivateParameterlessCtor(Sql.class));
    }

    @Test
    public void generate_sequence_of_placeholders() {
        final int count = 2;
        final String placeholders = Sql.nPlaceholders(count);
        final String expected =
                BRACKET_OPEN.toString()
                + PLACEHOLDER + COMMA
                + PLACEHOLDER
                + BRACKET_CLOSE;
        assertEquals(expected, placeholders);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fail_to_generate_placeholders_of_negative_count() {
        final int count = -1;
        Sql.nPlaceholders(count);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fail_to_generate_zero_placeholders() {
        final int count = 0;
        Sql.nPlaceholders(count);
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

        final String varcharExpected = "VARCHAR(512)";
        final String varcharActual = Sql.Type.VARCHAR_512.toString()
                                                         .trim();
        assertEquals(varcharExpected, varcharActual);
    }

    @Test
    public void provide_tokens_wrapped_into_whitespace() {
        final String sumExpected = " SUM ";
        final String sumActual = Sql.Function.SUM.toString();
        assertEquals(sumExpected, sumActual);

        final String blobExpected = " BLOB ";
        final String blobActual = Sql.Type.BLOB.toString();
        assertEquals(blobExpected, blobActual);

        final String primaryKeyExpected = " PRIMARY KEY ";
        final String primaryKeyActual = Sql.Query.PRIMARY_KEY.toString();
        assertEquals(primaryKeyExpected, primaryKeyActual);

        final String commaExpected = " , ";
        final String commaActual = Sql.BuildingBlock.COMMA.toString();
        assertEquals(commaExpected, commaActual);
    }
}
