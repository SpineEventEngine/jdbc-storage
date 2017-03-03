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

package org.spine3.server.storage.jdbc.util;

import com.google.protobuf.Message;
import org.junit.Test;
import org.spine3.server.entity.AbstractEntity;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.test.entity.ProjectId;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.spine3.server.storage.jdbc.Sql.Type.BIGINT;
import static org.spine3.server.storage.jdbc.Sql.Type.INT;
import static org.spine3.server.storage.jdbc.Sql.Type.VARCHAR_999;

/**
 * @author Dmytro Dashenkov.
 */
public class IdColumnShould {

    @Test
    public void have_bigint_impl() {
        final IdColumn<?> column = IdColumn.newInstance(LongIdEntity.class);
        assertEquals(BIGINT, column.getColumnDataType());
    }

    @Test
    public void have_int_impl() {
        final IdColumn<?> column = IdColumn.newInstance(IntIdEntity.class);
        assertEquals(INT, column.getColumnDataType());
    }

    @Test
    public void have_varchar999_impl() {
        final IdColumn<?> column = IdColumn.newInstance(StringIdEntity.class);
        assertEquals(VARCHAR_999, column.getColumnDataType());
    }

    @Test
    public void cast_message_IDs_to_string() {
        final IdColumn<?> column = IdColumn.newInstance(MessageIdEntity.class);
        assertEquals(VARCHAR_999, column.getColumnDataType());
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_fail_to_set_int_id() throws SQLException {
        final IdColumn<Integer> column = IdColumn.newInstance(IntIdEntity.class);
        column.setId(1, 1, faultyStatement());
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_fail_to_set_long_id() throws SQLException {
        final IdColumn<Long> column = IdColumn.newInstance(LongIdEntity.class);
        column.setId(1, 1L, faultyStatement());
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_fail_to_set_string_id() throws SQLException {
        final IdColumn<String> column = IdColumn.newInstance(StringIdEntity.class);
        column.setId(1, "bazinga!", faultyStatement());
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_fail_to_set_message_id() throws SQLException {
        final IdColumn<ProjectId> column = IdColumn.newInstance(MessageIdEntity.class);
        column.setId(1, ProjectId.getDefaultInstance(), faultyStatement());
    }

    private static PreparedStatement faultyStatement() throws SQLException {
        final PreparedStatement statement = mock(PreparedStatement.class);
        final Exception exception = new SQLException("Faulty statement causes failures");
        doThrow(exception).when(statement).setString(anyInt(), anyString());
        doThrow(exception).when(statement).setLong(anyInt(), anyLong());
        doThrow(exception).when(statement).setInt(anyInt(), anyInt());
        return statement;
    }

    private static class LongIdEntity extends AbstractEntity<Long, Message> {
        protected LongIdEntity(Long id) {
            super(id);
        }
    }

    private static class IntIdEntity extends AbstractEntity<Integer, Message> {
        protected IntIdEntity(Integer id) {
            super(id);
        }
    }

    private static class StringIdEntity extends AbstractEntity<String, Message> {
        protected StringIdEntity(String id) {
            super(id);
        }
    }

    private static class MessageIdEntity extends AbstractEntity<ProjectId, Message> {
        protected MessageIdEntity(ProjectId id) {
            super(id);
        }
    }
}
