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

package io.spine.server.storage.jdbc.config;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.spine.annotation.SPI;
import io.spine.query.ColumnName;
import io.spine.server.storage.jdbc.record.column.ColumnSpec;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Abstract base for the descriptions of the columns, which require some specific way
 * of storing their values in RDBMS.
 *
 * <p>The library users should provide their custom column specifications for the types
 * of the stored records, by extending this type.
 *
 * @see io.spine.server.storage.jdbc.JdbcStorageFactory.Builder#configureColumns(Class,
 *         CustomColumns)
 */
@SPI
@SuppressWarnings("AbstractClassWithoutAbstractMethods")    /* By design. */
public abstract class CustomColumns<R extends Message> {

    private final ImmutableList<ColumnSpec<R, ?>> columns;

    /**
     * Creates the instance of this type with the given column specifications.
     */
    protected CustomColumns(ImmutableList<ColumnSpec<R, ?>> columns) {
        this.columns = columns;
    }

    /**
     * Returns the column specifications.
     */
    public Iterable<ColumnSpec<R, ?>> columns() {
        return columns;
    }

    /**
     * Returns the specification of the column by the column name,
     * or {@code null}, if no custom column definition was configured.
     */
    public @Nullable ColumnSpec<R, ?> find(ColumnName name) {
        return columns.stream()
                .filter(c -> c.name()
                              .equals(name))
                .findFirst()
                .orElse(null);
    }
}