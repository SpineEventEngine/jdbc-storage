/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import io.spine.annotation.Internal;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.Type;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.json.Json.toCompactJson;
import static io.spine.server.entity.model.EntityClass.asEntityClass;

/**
 * A helper class for setting the {@link Entity} ID into {@linkplain Parameters query parameters}.
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
    static <I> IdColumn<I> newInstance(Class<? extends Entity<I, ?>> entityClass,
                                       String columnName) {
        IdColumn<I> helper;
        Class<?> idClass = asEntityClass(entityClass).getIdClass();
        if (idClass == Long.class) {
            helper = (IdColumn<I>) new LongIdColumn(columnName);
        } else if (idClass == Integer.class) {
            helper = (IdColumn<I>) new IntIdColumn(columnName);
        } else if (idClass == String.class) {
            helper = (IdColumn<I>) new StringIdColumn(columnName);
        } else {
            Class<? extends Message> messageClass = (Class<? extends Message>) idClass;
            helper = (IdColumn<I>) new MessageIdColumn(messageClass, columnName);
        }
        return helper;
    }

    /**
     * Creates a {@link StringIdColumn} with the specified column name.
     *
     * @param columnName the name of the ID column
     * @return the {@code IdColumn}
     */
    public static IdColumn<String> typeString(String columnName) {
        return new StringIdColumn(columnName);
    }

    protected IdColumn(String columnName) {
        this.columnName = checkNotNull(columnName);
    }

    /**
     * Returns the {@link Type} of the column with which this helper instance works.
     */
    public abstract Type getSqlType();

    /**
     * Retrieves the {@linkplain Class Java class} of the ID before
     * {@linkplain #normalize(Object) normalization}.
     */
    public abstract Class<I> getJavaType();

    /**
     * Normalizes the identifier before setting it to a {@link Parameter}.
     *
     * <p>The method may perform a conversion of the ID to a type, which is more suitable
     * for storing. E.g. it may be useful to store a {@linkplain Message Protobuf Message}
     * in a JSON representation as a {@code String}.
     *
     * <p>If an ID type is a simple type as {@code String}, {@code Integer}, etc
     * the method may return the same value.
     *
     * @param id the identifier to normalize
     * @return the normalized ID
     */
    public abstract Object normalize(I id);

    /**
     * {@linkplain #normalize(Object) Normalizes} the specified IDs.
     *
     * @param ids the IDs to normalize
     * @return the normalized IDs
     */
    public Collection<Object> normalize(Iterable<I> ids) {
        Collection<Object> result = newLinkedList();
        for (I id : ids) {
            Object normalizedId = normalize(id);
            result.add(normalizedId);
        }
        return result;
    }

    public String getColumnName() {
        return columnName;
    }

    /**
     * Sets an ID parameter to the given value.
     *
     * @param idName     the name of the ID
     * @param id         the ID value to set
     * @param parameters the parameters to set the ID
     */
    public void setId(String idName, I id, Parameters.Builder parameters) {
        Object normalizedId = normalize(id);
        Parameter parameter = Parameter.of(normalizedId);
        parameters.addParameter(idName, parameter);
    }

    /**
     * Helps to work with columns which contain {@code long} {@link Entity} IDs.
     */
    private static class LongIdColumn extends IdColumn<Long> {

        private LongIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Type getSqlType() {
            return Type.LONG;
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
    private static class IntIdColumn extends IdColumn<Integer> {

        private IntIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Type getSqlType() {
            return Type.INT;
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
    private abstract static class StringOrMessageIdColumn<I> extends IdColumn<I> {

        private StringOrMessageIdColumn(String columnName) {
            super(columnName);
        }

        @Override
        public Type getSqlType() {
            return Type.STRING_255;
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
