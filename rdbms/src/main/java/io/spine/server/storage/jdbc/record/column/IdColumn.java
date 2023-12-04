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
import io.spine.annotation.Internal;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.Entity;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.server.entity.model.EntityClass.asEntityClass;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.server.storage.jdbc.Type.STRING_512;
import static io.spine.type.Json.toCompactJson;
import static io.spine.util.Exceptions.newIllegalArgumentException;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper for the column which stores a primary key in a DB {@linkplain RecordTable table}.
 *
 * @param <I>
 *         the ID type
 */
@Internal
public abstract class IdColumn<I> {

    public static final String ID_COLUMN_NAME = "ID";

    /**
     * The underlying {@link TableColumn storage field}.
     */
    private final TableColumn column;

    private IdColumn(TableColumn column) {
        this.column = column;
    }

    /**
     * Wraps a given ID {@code column} of a primitive type.
     *
     * <p>The column should have a valid SQL type.
     *
     * <p>This also implies that actual ID values will be primitive and matching to the column SQL
     * type.
     *
     * @see #of(TableColumn, Class) for {@link Message}-type ID column creation
     * @see #ofEntityClass(TableColumn, Class) for {@link Entity} ID column creation
     */
    @SuppressWarnings("unchecked") // It's up to caller to keep the ID class and SQL type in sync.
    public static <I> IdColumn<I> of(TableColumn column) {
        checkNotNull(column);
        var type = requireNonNull(column.type(),
                                  "Please use other suitable method overload if ID column SQL " +
                                          "type is unknown at compile time");
        switch (type) {
            case INT:
                return (IdColumn<I>) new IntIdColumn(column);
            case LONG:
                return (IdColumn<I>) new LongIdColumn(column);
            case STRING_512:
            case STRING_255:
            case STRING:
                return (IdColumn<I>) new StringIdColumn(column);
            case BOOLEAN:
            case BYTE_ARRAY:
            default:
                throw newIllegalArgumentException("Unexpected ID column SQL type: %s",
                                                  type);
        }
    }

    /**
     * Creates a new ID column for the passed record specification.
     *
     * @param spec
     *         specification of the record to store
     * @param mapping
     *         column type mapping
     * @param <I>
     *         the type of identifiers stored in the created column
     */
    @SuppressWarnings({
            "unchecked", // ID runtime type is checked with if statements.
            "IfStatementWithTooManyBranches", // OK for a factory method.
            "ChainOfInstanceofChecks"         // which depends on the built object target type.
    })
    public static <I> IdColumn<I> of(RecordSpec<I, ?> spec, JdbcColumnMapping mapping) {
        checkNotNull(spec);
        checkNotNull(mapping);
        var idType = spec.idType();
        var column = new TableColumn(ID_COLUMN_NAME, idType, mapping);
        if (idType == Long.class) {
            return (IdColumn<I>) new LongIdColumn(column);
        } else if (idType == Integer.class) {
            return (IdColumn<I>) new IntIdColumn(column);
        } else if (idType == String.class) {
            return (IdColumn<I>) new StringIdColumn(column);
        } else if (Message.class.isAssignableFrom(idType)) {
            var messageClass = (Class<? extends Message>) idType;
            return (IdColumn<I>) new MessageIdColumn<>(column, messageClass);
        } else {
            throw newIllegalArgumentException("Unexpected entity ID class %s", idType.getName());
        }
    }

    /**
     * Wraps a given ID {@code column} of {@link Message} type.
     *
     * <p>The column should have an SQL type set to {@code null}.
     */
    public static <I extends Message> IdColumn<I> of(TableColumn column, Class<I> messageClass) {
        checkNotNull(column);
        checkNotNull(messageClass);
        checkArgument(column.type() == null,
                      "Message-type IDs follow the predefined conversion rules of `IdColumn` " +
                              "and shouldn't have an SQL type set on their own");
        return new MessageIdColumn<>(column, messageClass);
    }

    /**
     * Wraps a given {@link Entity} ID column.
     *
     * <p>The column should have an SQL type set to {@code null}.
     *
     * <p>The column type is calculated at runtime according to the generic parameters of the
     * {@code Entity} {@linkplain io.spine.server.entity.model.EntityClass class}.
     *
     * @param entityClass
     *         a class of an {@link Entity} or an {@link Aggregate}
     * @param <I>
     *         the type of {@link Entity} IDs
     * @return a new helper instance
     */
    @SuppressWarnings({
            "unchecked", // ID runtime type is checked with if statements.
            "IfStatementWithTooManyBranches", // OK for a factory method
            "ChainOfInstanceofChecks"         // which depends on the built object target type.
    })
    public static <I> IdColumn<I>
    ofEntityClass(TableColumn column, Class<? extends Entity<I, ?>> entityClass) {
        checkNotNull(column);
        checkNotNull(entityClass);
        checkArgument(column.type() == null,
                      "Entity ID type is calculated at runtime and shouldn't have an SQL type " +
                              "pre-set");
        var idClass = asEntityClass(entityClass).idClass();
        if (idClass == Long.class) {
            return (IdColumn<I>) new LongIdColumn(column);
        } else if (idClass == Integer.class) {
            return (IdColumn<I>) new IntIdColumn(column);
        } else if (idClass == String.class) {
            return (IdColumn<I>) new StringIdColumn(column);
        } else if (Message.class.isAssignableFrom(idClass)) {
            var messageClass = (Class<? extends Message>) idClass;
            return (IdColumn<I>) new MessageIdColumn<>(column, messageClass);
        } else {
            throw newIllegalArgumentException("Unexpected entity ID class `%s`.",
                                              idClass.getName());
        }
    }

    /**
     * Returns the {@link Type} of the column with which this helper instance works.
     */
    public abstract Type sqlType();

    /**
     * Retrieves the {@linkplain Class Java class} of the ID before
     * {@linkplain #normalize(Object) normalization}.
     */
    public abstract Class<I> javaType();

    /**
     * Normalizes the identifier before using it as a parameter value.
     *
     * <p>The method may perform a conversion of the ID to a type, which is more suitable
     * for storing. E.g. it may be useful to store a {@linkplain Message Protobuf Message}
     * in a JSON representation as a {@code String}.
     *
     * <p>If an ID type is a simple type as {@code String}, {@code Integer}, etc.
     * the method may return the same value.
     *
     * @param id
     *         the identifier to normalize
     * @return the normalized ID
     */
    public abstract Object normalize(I id);

    /**
     * {@linkplain #normalize(Object) Normalizes} the specified IDs.
     *
     * @param ids
     *         the IDs to normalize
     * @return the normalized IDs
     */
    public Collection<Object> normalize(Iterable<I> ids) {
        Collection<Object> result = newLinkedList();
        for (var id : ids) {
            var normalizedId = normalize(id);
            result.add(normalizedId);
        }
        return result;
    }

    /**
     * Returns the {@code String} value of the column name.
     */
    public String columnName() {
        return column().name();
    }

    /**
     * Returns the table column corresponding to this ID column.
     */
    public TableColumn column() {
        return column;
    }

    /**
     * An ID column storing {@code Long}s.
     */
    private static class LongIdColumn extends IdColumn<Long> {

        private LongIdColumn(TableColumn column) {
            super(column);
        }

        @Override
        public Type sqlType() {
            return LONG;
        }

        @Override
        public Class<Long> javaType() {
            return Long.class;
        }

        @Override
        public Long normalize(Long id) {
            return id;
        }
    }

    /**
     * An ID column containing only {@code Integer}s.
     */
    private static class IntIdColumn extends IdColumn<Integer> {

        private IntIdColumn(TableColumn column) {
            super(column);
        }

        @Override
        public Type sqlType() {
            return INT;
        }

        @Override
        public Class<Integer> javaType() {
            return Integer.class;
        }

        @Override
        public Integer normalize(Integer id) {
            return id;
        }
    }

    /**
     * An ID column which stores {@code String}s.
     */
    private static class StringIdColumn extends IdColumn<String> {

        private StringIdColumn(TableColumn column) {
            super(column);
        }

        @Override
        public String normalize(String id) {
            return id;
        }

        @Override
        public Type sqlType() {
            return STRING_512;
        }

        @Override
        public Class<String> javaType() {
            return String.class;
        }
    }

    /**
     * An ID column, which stores Proto messages of type {@code M}.
     *
     * @param <M>
     *         type of the stored values
     */
    private static class MessageIdColumn<M extends Message> extends IdColumn<M> {

        private final Class<M> cls;

        private MessageIdColumn(TableColumn column, Class<M> cls) {
            super(column);
            this.cls = cls;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Converts the given {@link Message} ID into its
         * {@linkplain io.spine.type.Json#toCompactJson JSON representation}.
         */
        @Override
        public String normalize(M id) {
            return toCompactJson(id);
        }

        @Override
        public Type sqlType() {
            return STRING_255;
        }

        @Override
        public Class<M> javaType() {
            return cls;
        }
    }
}
