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

package io.spine.server.storage.jdbc.util;

import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.Identifier;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A helper class for setting the {@link Entity} ID into a {@link PreparedStatement}as a query
 * parameter.
 *
 * <p>Depending on what type ID is, {@linkplain #setId(int, Object, PreparedStatement) setId} method
 * will call one of the setters:
 * <ul>
 *     <li>{@link PreparedStatement#setInt}
 *     <li>{@link PreparedStatement#setLong}
 *     <li>{@link PreparedStatement#setString}
 * </ul>
 *
 * @param <I> the type of {@link Entity} IDs
 * @author Alexander Litus
 */
@Internal
public abstract class IdColumn<I> {

    private final String columnName;

    /**
     * Creates a new instance.
     *
     * @param entityClass a class of an {@link Entity} or an {@link Aggregate}
     * @param <I>         the type of {@link Entity} IDs
     * @return a new helper instance
     */
    public static <I> IdColumn<I> newInstance(Class<? extends Entity<I, ?>> entityClass,
                                              String columnName) {
        final IdColumn<I> helper;
        final Class<I> idClass = Entity.TypeInfo.getIdClass(entityClass);
        if (idClass.equals(Long.class)) {
            @SuppressWarnings("unchecked") // is checked already
            final IdColumn<I> longIdColumn =
                    (IdColumn<I>) new LongIdColumn(columnName);
            helper = longIdColumn;
        } else if (idClass.equals(Integer.class)) {
            @SuppressWarnings("unchecked") // is checked already
            final IdColumn<I> intIdColumn =
                    (IdColumn<I>) new IntIdColumn(columnName);
            helper = intIdColumn;
        } else {
            helper = new StringOrMessageIdColumn<>(columnName);
        }
        return helper;
    }

    public static IdColumn<String> typeString(String columnName) {
        return new StringIdColumn(columnName);
    }

    protected IdColumn(String columnName) {
        this.columnName = checkNotNull(columnName);
    }

    /**
     * Returns the {@link Sql.Type} of the column with which this helper instance works.
     */
    public abstract Sql.Type getSqlType();

    public abstract Class<I> getJavaType();

    public String getColumnName() {
        return columnName;
    }

    /**
     * Sets an ID parameter to the given value.
     *
     * @param index     the ID parameter index
     * @param id        the ID value to set
     * @param statement the statement to use
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    public abstract void setId(int index, I id, PreparedStatement statement)
            throws DatabaseException;

    /**
     * Helps to work with columns which contain {@code long} {@link Entity} IDs.
     */
    private static class LongIdColumn extends IdColumn<Long> {

        private LongIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BIGINT;
        }

        @Override
        public Class<Long> getJavaType() {
            return Long.class;
        }

        @Override
        public void setId(int index, Long id, PreparedStatement statement)
                throws DatabaseException {
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

        private IntIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.INT;
        }

        @Override
        public Class<Integer> getJavaType() {
            return Integer.class;
        }

        @Override
        public void setId(int index, Integer id, PreparedStatement statement)
                throws DatabaseException {
            try {
                statement.setInt(index, id);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * Helps to work with columns which contain either {@link Message} or {@code string}
     * {@link Entity} IDs.
     */
    private static class StringOrMessageIdColumn<I> extends IdColumn<I> {

        private StringOrMessageIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_255;
        }

        @Override
        public Class<I> getJavaType() {
            return (Class<I>) String.class; // TODO:2017-07-18:dmytro.dashenkov: Revisit.
        }

        @Override
        public void setId(int index, I id, PreparedStatement statement) throws DatabaseException {
            final String idString = Identifier.toString(id);
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

        private StringIdColumn(String columnName) {
            super(columnName);
        }
    }
}
