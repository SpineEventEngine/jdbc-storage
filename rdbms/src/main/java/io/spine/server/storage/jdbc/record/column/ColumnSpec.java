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
import io.spine.query.ColumnName;
import io.spine.query.RecordColumn;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * Tells how the column of a record should be stored in RDBMS.
 *
 * @param <R>
 *         the type of the stored record
 * @param <V>
 *         the type of the column values
 */
public class ColumnSpec<R extends Message, V> {

    private final RecordColumn<R, V> column;

    public ColumnSpec(RecordColumn<R, V> column) {
        this.column = column;
    }

    public ColumnName name() {
        return column.name();
    }

    RecordColumn<R, V> column() {
        return column;
    }

    public Object transformValue(V value) {
        return value;
    }

    @SuppressWarnings("unchecked")  /* This is a responsibility of caller. */
    public Function<Object, Object> transformFn() {
        return (value) -> transformValue((V) value);
    }

    public static <R extends Message, V> ColumnSpec<R, V>
    columnSpec(RecordColumn<R, V> column, Function<V, Object> transform) {
        checkNotNull(column);
        return new ColumnSpec<>(column) {
            @Override
            public Object transformValue(V value) {
                Object result;
                try {
                    result = transform.apply(value);
                } catch (RuntimeException e) {
                    throw newIllegalStateException(
                            e, "Cannot transform the value for the column `%s`.",
                            column().name());
                }
                return result;
            }
        };
    }
}
