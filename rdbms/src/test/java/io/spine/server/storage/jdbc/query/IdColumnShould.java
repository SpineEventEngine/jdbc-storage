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

import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import io.spine.server.entity.AbstractEntity;
import io.spine.test.entity.ProjectId;
import org.junit.Test;

import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmytro Dashenkov
 */
public class IdColumnShould {

    private static final String ID = "id";

    @Test
    public void have_bigint_impl() {
        final IdColumn<?> column = IdColumn.newInstance(LongIdEntity.class, ID);
        assertEquals(LONG, column.getSqlType());
        assertSame(Long.class, column.getJavaType());
    }

    @Test
    public void have_int_impl() {
        final IdColumn<?> column = IdColumn.newInstance(IntIdEntity.class, ID);
        assertEquals(INT, column.getSqlType());
        assertSame(Integer.class, column.getJavaType());
    }

    @Test
    public void have_varchar255_impl() {
        final IdColumn<?> column = IdColumn.newInstance(StringIdEntity.class, ID);
        assertEquals(STRING_255, column.getSqlType());
        assertSame(String.class, column.getJavaType());
    }

    @Test
    public void cast_message_IDs_to_string() {
        final IdColumn<?> column = IdColumn.newInstance(MessageIdEntity.class, ID);
        assertEquals(STRING_255, column.getSqlType());
        assertTrue(Message.class.isAssignableFrom(column.getJavaType()));
    }

    @Test
    public void store_column_name() {
        final IdColumn<String> column = IdColumn.newInstance(StringIdEntity.class, ID);
        assertEquals(ID, column.getColumnName());
    }

    private static class LongIdEntity extends AbstractEntity<Long, StringValue> {
        protected LongIdEntity(Long id) {
            super(id);
        }
    }

    private static class IntIdEntity extends AbstractEntity<Integer, StringValue> {
        protected IntIdEntity(Integer id) {
            super(id);
        }
    }

    private static class StringIdEntity extends AbstractEntity<String, StringValue> {
        protected StringIdEntity(String id) {
            super(id);
        }
    }

    private static class MessageIdEntity extends AbstractEntity<ProjectId, StringValue> {
        protected MessageIdEntity(ProjectId id) {
            super(id);
        }
    }
}
