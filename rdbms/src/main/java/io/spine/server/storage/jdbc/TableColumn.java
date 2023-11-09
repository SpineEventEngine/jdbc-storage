/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc;

import io.spine.query.ColumnName;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Function;

import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * A representation of a table column in RDBMS.
 */
public class TableColumn {

    private final String name;
    private final JdbcColumnMapping mapping;
    private final Class<?> type;
    private final @Nullable Function<@Nullable Object, @Nullable Object> adaptValue;

    public TableColumn(String name, Class<?> type, JdbcColumnMapping mapping) {
        this.name = name;
        this.type = type;
        this.mapping = mapping;
        this.adaptValue = null;
    }

    public TableColumn(String name, Class<?> valueType, JdbcColumnMapping mapping,
                       Function<@Nullable Object, @Nullable Object> adaptValue) {
        this.name = name;
        this.type = valueType;
        this.mapping = mapping;
        this.adaptValue = adaptValue;
    }

    /**
     * The column name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the {@linkplain Type SQL type} of the column,
     * or {@code null} if the type is unknown at compile-time.
     */
    public @Nullable Type type() {
        return mapping.typeOf(type);
    }

    /**
     * Returns {@code true} if this column may contain {@code NULL} values, {@code false} otherwise.
     *
     * <p>By default, returns {@code false}.
     */
    public boolean isNullable() {
        return false;
    }

    public @Nullable Object valueIn(RecordWithColumns<?, ?> record) {
        var columnName = ColumnName.of(name());
        if(!record.hasColumn(columnName)) {
            throw newIllegalArgumentException(
                    "Cannot find the column `%s` in record-with-columns of type `%s`.",
                    name(), record.record().getClass()
                    );
        }
        var value = record.columnValue(columnName, mapping);
        @Nullable Object result = adaptValue(value);
        return result;
    }

    private @Nullable Object adaptValue(Object original) {
        @Nullable Object result = original;
        if (adaptValue != null) {
            result = adaptValue.apply(original);
        }
        return result;
    }
}
