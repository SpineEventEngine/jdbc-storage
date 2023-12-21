/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record.column;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.record.column.given.IdColumnTestEnv.IntIdEntity;
import io.spine.server.storage.jdbc.record.column.given.IdColumnTestEnv.LongIdEntity;
import io.spine.server.storage.jdbc.record.column.given.IdColumnTestEnv.MessageIdEntity;
import io.spine.server.storage.jdbc.record.column.given.IdColumnTestEnv.StringIdEntity;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.entity.storage.SpecScanner.scan;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING_512;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("`IdColumn` should")
class IdColumnTest {

    private static final JdbcColumnMapping mapping = new JdbcColumnMapping();

    @Test
    @DisplayName("allow `LONG` SQL type")
    void haveBigintImpl() {
        IdColumn<?> column = IdColumn.of(scan(LongIdEntity.class), mapping);
        assertEquals(LONG, column.sqlType());
        assertSame(Long.class, column.javaType());
    }

    @Test
    @DisplayName("allow `INT` SQL type")
    void haveIntImpl() {
        IdColumn<?> column = IdColumn.of(scan(IntIdEntity.class), mapping);
        assertEquals(INT, column.sqlType());
        assertSame(Integer.class, column.javaType());
    }

    @Test
    @DisplayName("allow `VARCHAR_512` SQL type")
    void haveStringImpl() {
        IdColumn<?> column = IdColumn.of(scan(StringIdEntity.class), mapping);
        assertEquals(STRING_512, column.sqlType());
        assertSame(String.class, column.javaType());
    }

    @Test
    @DisplayName("cast message IDs to string")
    void castMessageIdsToString() {
        IdColumn<?> column = IdColumn.of(scan(MessageIdEntity.class), mapping);
        assertEquals(STRING_512, column.sqlType());
        assertTrue(Message.class.isAssignableFrom(column.javaType()));
    }
}
