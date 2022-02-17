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

package io.spine.server.storage.jdbc;

import io.spine.testing.UtilityClassTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.testing.Assertions.assertHasPrivateParameterlessCtor;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("`Sql` utility should")
class SqlTest extends UtilityClassTest<Sql> {

    SqlTest() {
        super(Sql.class);
    }

    @SuppressWarnings("DuplicateStringLiteralInspection")
    @Test
    @DisplayName("provide valid SQL tokens")
    void provideValidSqlTokens() {
        var createTableExpected = "CREATE TABLE";
        var createTableActual = Sql.Query.CREATE_TABLE.toString()
                                                      .trim();
        assertEquals(createTableExpected, createTableActual);

        var countExpected = "COUNT";
        var countActual = Sql.Function.COUNT.toString()
                                            .trim();
        assertEquals(countExpected, countActual);
    }

    @Test
    @DisplayName("provide tokens wrapped into whitespace")
    void provideTokensWithWhitespaces() {
        var sumExpected = " SUM ";
        var sumActual = Sql.Function.SUM.toString();
        assertEquals(sumExpected, sumActual);

        var primaryKeyExpected = " PRIMARY KEY ";
        var primaryKeyActual = Sql.Query.PRIMARY_KEY.toString();
        assertEquals(primaryKeyExpected, primaryKeyActual);

        var commaExpected = " , ";
        var commaActual = Sql.BuildingBlock.COMMA.toString();
        assertEquals(commaExpected, commaActual);
    }
}
