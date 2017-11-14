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

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;

/**
 * Default {@link TypeMapping type mappings} for different databases.
 *
 * @author Dmytro Grankin
 */
@SuppressWarnings("DuplicateStringLiteralInspection") // Use literals for a better readability.
public class TypeMappings {

    private static final TypeMapping MY_SQL = TypeMapping.newBuilder()
                                                         .add(BYTE_ARRAY, "BLOB")
                                                         .add(INT, "INT")
                                                         .add(LONG, "BIGINT")
                                                         .add(STRING_255, "VARCHAR(255)")
                                                         .add(STRING, "TEXT")
                                                         .add(BOOLEAN, "BOOLEAN")
                                                         .build();

    private static final TypeMapping POSTGRES = TypeMapping.newBuilder()
                                                           .add(BYTE_ARRAY, "BYTEA")
                                                           .add(INT, "INT")
                                                           .add(LONG, "BIGINT")
                                                           .add(STRING_255, "VARCHAR(255)")
                                                           .add(STRING, "TEXT")
                                                           .add(BOOLEAN, "BOOLEAN")
                                                           .build();

    private TypeMappings() {
        // Prevent instantiation of this utility class.
    }

    /**
     * Obtains the default type mapping for MySQL database.
     */
    public static TypeMapping mySql() {
        return MY_SQL;
    }

    /**
     * Obtains the default type mapping for PostreSQL database.
     */
    public static TypeMapping postgreSql() {
        return POSTGRES;
    }
}
