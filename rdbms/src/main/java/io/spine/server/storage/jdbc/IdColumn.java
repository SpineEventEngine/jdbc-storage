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

import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityClass;
import io.spine.server.storage.jdbc.query.Parameters;

import java.sql.PreparedStatement;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.json.Json.toCompactJson;

/**
 * A helper class for setting the {@link Entity} ID into a {@link PreparedStatement}
 * as a query parameter.
 *
 * @param <I> the type of {@link Entity} IDs
 * @param <R> the type of IDs after {@linkplain #normalize(Object) normalization}
 * @author Alexander Litus
 */
@Internal
public abstract class IdColumn<I, R> {

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
    public static <I> IdColumn<I, ?> newInstance(Class<? extends Entity<I, ?>> entityClass,
                                                 String columnName) {
        final IdColumn<I, ?> helper;
        final Class<?> idClass = new EntityClass<Entity>(entityClass).getIdClass();
        if (idClass == Long.class) {
            helper = (IdColumn<I, Long>) new LongIdColumn(columnName);
        } else if (idClass == Integer.class) {
            helper = (IdColumn<I, Integer>) new IntIdColumn(columnName);
        } else if (idClass == String.class) {
            helper = (IdColumn<I, String>) new StringIdColumn(columnName);
        } else {
            final Class<? extends Message> messageClass = (Class<? extends Message>) idClass;
            helper = (IdColumn<I, String>) new MessageIdColumn(messageClass, columnName);
        }
        return helper;
    }

    public static IdColumn<String, String> typeString(String columnName) {
        return new StringIdColumn(columnName);
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

    /**
     * Normalizes the identifier before setting it to a {@link PreparedStatement}.
     *
     * <p>The method may perform a conversion, validation or no action.
     *
     * @param id the identifier to normalize
     * @return the normalized ID
     */
    protected abstract R normalize(I id);

    public String getColumnName() {
        return columnName;
    }

    /**
     * Sets an ID parameter to the given value.
     *
     * @param index      the ID parameter index
     * @param id         the ID value to set
     * @param parameters the parameters to set the ID
     */
    public void setId(int index, I id, Parameters.Builder parameters) {
        final R normalizedId = normalize(id);
        parameters.addParameter(index, normalizedId);
    }

    /**
     * Helps to work with columns which contain {@code long} {@link Entity} IDs.
     */
    private static class LongIdColumn extends IdColumn<Long, Long> {

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
        public Long normalize(Long id) {
            return id;
        }
    }

    /**
     * Helps to work with columns which contain {@code integer} {@link Entity} IDs.
     */
    private static class IntIdColumn extends IdColumn<Integer, Integer> {

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
        public Integer normalize(Integer id) {
            return id;
        }
    }

    /**
     * Helps to work with columns which contain either {@link Message} or {@code string}
     * {@link Entity} IDs.
     */
    private abstract static class StringOrMessageIdColumn<I> extends IdColumn<I, String> {

        private StringOrMessageIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_255;
        }
    }

    /**
     * Helps to work with columns which contain {@code String} {@link Entity} IDs.
     */
    private static class StringIdColumn extends StringOrMessageIdColumn<String> {

        private StringIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public String normalize(String id) {
            return id;
        }

        @Override
        public Class<String> getJavaType() {
            return String.class;
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
        public String normalize(M id) {
            return toCompactJson(id);
        }

        @Override
        public Class<M> getJavaType() {
            return cls;
        }
    }
}
