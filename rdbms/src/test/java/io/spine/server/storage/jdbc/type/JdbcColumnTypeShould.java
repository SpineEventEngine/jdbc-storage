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

package io.spine.server.storage.jdbc.type;

import io.spine.server.storage.jdbc.query.Parameter;
import io.spine.server.storage.jdbc.query.Parameters;
import io.spine.test.Tests;
import org.junit.Test;

import java.sql.SQLException;

import static io.spine.Identifier.newUuid;
import static org.junit.Assert.assertNull;

/**
 * @author Dmytro Dashenkov
 */
public class JdbcColumnTypeShould {

    private final JdbcColumnType<String, ?> columnType = JdbcColumnTypes.stringType();

    @Test
    public void set_null_to_parameters() throws SQLException {
        final String identifier = newUuid();
        final Parameters.Builder builder = Parameters.newBuilder();
        columnType.setNull(builder, identifier);

        final Parameters allParameters = builder.build();
        final Parameter parameter = allParameters.getParameter(identifier);
        assertNull(parameter.getValue());
    }

    @Test(expected = NullPointerException.class)
    public void check_converted_value_to_be_nonnull() {
        columnType.convertColumnValue(Tests.<String>nullRef());
    }
}
