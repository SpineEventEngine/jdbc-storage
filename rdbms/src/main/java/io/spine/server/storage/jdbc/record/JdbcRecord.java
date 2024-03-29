/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.query.ColumnName;
import io.spine.server.storage.RecordWithColumns;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A record to insert into an RDBMS-backed table.
 *
 * <p>Adapts the values held in a {@link RecordWithColumns} according
 * to a certain {@linkplain JdbcTableSpec table specification}.
 *
 * @param <I>
 *         type or record identifiers
 * @param <R>
 *         type of the record to insert
 */
public final class JdbcRecord<I, R extends Message> {

    private final RecordWithColumns<I, R> original;
    private final JdbcTableSpec<I, R> spec;
    private final ImmutableSet<ColumnName> columnNames;

    public JdbcRecord(JdbcTableSpec<I, R> spec, RecordWithColumns<I, R> recordWithCols) {
        this.spec = spec;
        this.original = recordWithCols;
        this.columnNames = spec.columnNames();
    }

    public I id() {
        return original.id();
    }

    public Iterable<ColumnName> columns() {
        return columnNames;
    }

    @Nullable
    public Object columnValue(ColumnName name) {
        @Nullable Object result = spec.valueIn(original, name);
        return result;
    }

    /**
     * Returns the original record-with-columns,
     * on top of which this {@code JdbcRecord} is created.
     */
    public RecordWithColumns<I, R> original() {
        return original;
    }

    /**
     * Returns a new instance of {@code JdbcRecord} around the specified {@code newRecord},
     * while leaving the other properties unchanged.
     *
     * @param newRecord
     *         the new record with columns
     * @return a new {@code JdbcRecord} instance with the specified newRecord
     */
    @Internal
    @CanIgnoreReturnValue
    public JdbcRecord<I, R> copyWithRecord(R newRecord) {
        var withCols = RecordWithColumns.create(newRecord, spec.recordSpec());
        return new JdbcRecord<>(spec, withCols);
    }
}
