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

package io.spine.server.storage.jdbc.type;

import io.spine.server.storage.ColumnTypeMapping;
import io.spine.server.storage.jdbc.Type;

/**
 * A single column type mapping which also stores the RDBMS type of the column.
 *
 * @param <T>
 *         the type of the original column values
 * @param <R>
 *         the type of the values to store in RDBMS
 */
public final class JdbcColumnTypeMapping<T, R> implements ColumnTypeMapping<T, R> {

    private final ColumnTypeMapping<T, R> mapping;
    private final Type type;

    public JdbcColumnTypeMapping(ColumnTypeMapping<T, R> mapping, Type type) {
        this.mapping = mapping;
        this.type = type;
    }

    @Override
    public R apply(T t) {
        try {
            return mapping.apply(t);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Error converting the column value.", e);
        }
    }

    public Type storeAs() {
        return type;
    }
}
