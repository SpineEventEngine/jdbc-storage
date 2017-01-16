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
import org.spine3.Internal;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.jdbc.Sql.Type.BIGINT;
import static org.spine3.server.storage.jdbc.Sql.Type.INT;
import static org.spine3.server.storage.jdbc.Sql.Type.VARCHAR_999;

/**
 * Helps to work with {@link Entity} ID columns.
 *
 * @param <I> the type of {@link Entity} IDs
 * @author Alexander Litus
 */
@Internal
public abstract class IdColumn<I> {

    /**
     * Creates a new instance.
     *
     * @param entityClass a class of an {@link Entity} or an {@link Aggregate}
     * @param <I>         the type of {@link Entity} IDs
     * @return a new helper instance
     */
    public static <I> IdColumn<I> newInstance(Class<? extends Entity<I, ?>> entityClass) {
        final IdColumn<I> helper;
        final Class<I> idClass = Entity.getIdClass(entityClass);
        if (idClass.equals(Long.class)) {
            @SuppressWarnings("unchecked") // is checked already
            final IdColumn<I> longIdColumn = (IdColumn<I>) new LongIdColumn();
            helper = longIdColumn;
        } else if (idClass.equals(Integer.class)) {
            @SuppressWarnings("unchecked") // is checked already
            final IdColumn<I> intIdColumn = (IdColumn<I>) new IntIdColumn();
            helper = intIdColumn;
        } else {
            helper = new StringOrMessageIdColumn<>();
        }
        return helper;
    }

    /**
     * Returns the SQL data type string of the ID column, e.g. {@code "BIGINT"}, {@code "VARCHAR(999)"}, etc.
     */
    public abstract String getColumnDataType();

    /**
     * Sets an ID parameter to the given value.
     *
     * @param index     the ID parameter index
     * @param id        the ID value to set
     * @param statement the statement to use
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    public abstract void setId(int index, I id, PreparedStatement statement) throws DatabaseException;

    /**
     * Helps to work with columns which contain {@code long} {@link Entity} IDs.
     */
    private static class LongIdColumn extends IdColumn<Long> {

        @Override
        public String getColumnDataType() {
            return BIGINT.toString();
        }

        @Override
        public void setId(int index, Long id, PreparedStatement statement) throws DatabaseException {
            try {
                statement.setLong(index, id);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * Helps to work with columns which contain {@code integer} {@link Entity} IDs.
     */
    private static class IntIdColumn extends IdColumn<Integer> {

        @Override
        public String getColumnDataType() {
            return INT.toString();
        }

        @Override
        public void setId(int index, Integer id, PreparedStatement statement) throws DatabaseException {
            try {
                statement.setInt(index, id);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * Helps to work with columns which contain either {@link Message} or {@code string} {@link Entity} IDs.
     */
    private static class StringOrMessageIdColumn<I> extends IdColumn<I> {

        @Override
        public String getColumnDataType() {
            return VARCHAR_999.toString();
        }

        @Override
        public void setId(int index, I id, PreparedStatement statement) throws DatabaseException {
            final String idString = idToString(id);
            try {
                statement.setString(index, idString);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * Helps to work with columns which contain {@code string} {@link Entity} IDs.
     */
    public static class StringIdColumn extends StringOrMessageIdColumn<String> {
    }
}
