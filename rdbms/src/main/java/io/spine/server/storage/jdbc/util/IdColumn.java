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
import io.spine.Identifier;
import io.spine.annotation.Internal;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.primitives.Primitives.wrap;
import static io.spine.json.Json.toCompactJson;

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
    @SuppressWarnings({
            "unchecked", // ID runtime type is checked with if statements.
            "IfStatementWithTooManyBranches", // OK for a factory method
            "ChainOfInstanceofChecks"         // which depends on the built object target type.
    })
    public static <I> IdColumn<I> newInstance(Class<? extends Entity<I, ?>> entityClass,
                                              String columnName) {
        final IdColumn<I> helper;
        final Class<I> idClass = wrap(Entity.TypeInfo.<I>getIdClass(entityClass));
        if (idClass == Long.class) {
            helper = (IdColumn<I>) new LongIdColumn(columnName);
        } else if (idClass == Integer.class) {
            helper = (IdColumn<I>) new IntIdColumn(columnName);
        } else if (idClass == String.class) {
            helper = (IdColumn<I>) new StringIdColumn(columnName);
        } else {
            final Class<? extends Message> messageClass = (Class<? extends Message>) idClass;
            helper = (IdColumn<I>) new MessageIdColumn(messageClass, columnName);
        }
        return helper;
    }

    public static IdColumn<String> typeString(String columnName) {
        return new StringIdColumn<>(columnName);
    }

    protected IdColumn(String columnName) {
        this.columnName = checkNotNull(columnName);
    }

    /**
     * Returns the {@link Sql.Type} of the column with which this helper instance works.
     */
    public abstract Sql.Type getSqlType();

    /**
     * Retrieves the {@linkplain Class Java class} of the ID when it's being set to
     * the {@link PreparedStatement}.
     */
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
    private abstract static class StringOrMessageIdColumn<I> extends IdColumn<I> {

        private StringOrMessageIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_255;
        }

        @Override
        public void setId(int index, I id, PreparedStatement statement) throws DatabaseException {
            final String idString = normalize(id);
            try {
                statement.setString(index, idString);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        /**
         * Normalizes the identifier before setting it to a {@link PreparedStatement}.
         *
         * <p>The method may perform a {@code String} conversion, validation or no action for
         * a {@code String} input.
         *
         * @param id the identifier to convert normalize
         * @return the normalized {@code String} ID
         */
        protected abstract String normalize(I id);
    }

    /**
     * Helps to work with columns which contain {@code string} {@link Entity} IDs.
     *
     * <p>This class may serve as a stub for unknown ID types by considering their string
     * representation. See {@link Identifier#toString(Object) Identifier.toString(I)}.
     */
    private static class StringIdColumn<I> extends StringOrMessageIdColumn<I> {

        private StringIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        protected String normalize(I id) {
            return (String) id;
        }

        @SuppressWarnings("unchecked") // Logically checked.
        @Override
        public Class<I> getJavaType() {
            return (Class<I>) String.class;
        }
    }

    private static class MessageIdColumn<M extends Message> extends StringOrMessageIdColumn<M> {

        private final Class<M> cls;

        private MessageIdColumn(Class<M> cls, String columnName) {
            super(columnName);
            this.cls = cls;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Converts the given {@link Message} ID into its
         * {@linkplain io.spine.json.Json#toCompactJson JSON representation}.
         */
        @Override
        protected String normalize(M id) {
            return toCompactJson(id);
        }

        @Override
        public Class<M> getJavaType() {
            return cls;
        }
    }
}
