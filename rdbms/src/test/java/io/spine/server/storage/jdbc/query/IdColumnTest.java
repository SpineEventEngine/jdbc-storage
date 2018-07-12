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

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.query.given.IdColumnTestEnv.IntIdEntity;
import io.spine.server.storage.jdbc.query.given.IdColumnTestEnv.LongIdEntity;
import io.spine.server.storage.jdbc.query.given.IdColumnTestEnv.MessageIdEntity;
import io.spine.server.storage.jdbc.query.given.IdColumnTestEnv.StringIdEntity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("IdColumn should")
class IdColumnTest {

    private static final String ID = "ID";

    @Test
    @DisplayName("have `bigint` implementation")
    void haveBigintImpl() {
        IdColumn<?> column = IdColumn.newInstance(LongIdEntity.class, ID);
        assertEquals(LONG, column.getSqlType());
        assertSame(Long.class, column.getJavaType());
    }

    @Test
    @DisplayName("have `int` implementation")
    void haveIntImpl() {
        IdColumn<?> column = IdColumn.newInstance(IntIdEntity.class, ID);
        assertEquals(INT, column.getSqlType());
        assertSame(Integer.class, column.getJavaType());
    }

    @Test
    @DisplayName("have `varchar255` implementation")
    void haveStringImpl() {
        IdColumn<?> column = IdColumn.newInstance(StringIdEntity.class, ID);
        assertEquals(STRING_255, column.getSqlType());
        assertSame(String.class, column.getJavaType());
    }

    @Test
    @DisplayName("cast message IDs to string")
    void castMessageIdsToString() {
        IdColumn<?> column = IdColumn.newInstance(MessageIdEntity.class, ID);
        assertEquals(STRING_255, column.getSqlType());
        assertTrue(Message.class.isAssignableFrom(column.getJavaType()));
    }

    @Test
    @DisplayName("store column name")
    void storeColumnName() {
        IdColumn<String> column = IdColumn.newInstance(StringIdEntity.class, ID);
        assertEquals(ID, column.getColumnName());
    }
}
