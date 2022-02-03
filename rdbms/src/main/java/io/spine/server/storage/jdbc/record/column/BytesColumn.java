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

import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.record.Serializer;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.checkerframework.checker.nullness.qual.Nullable;

import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.record.Serializer.serialize;

/**
 * A column responsible for storing serialized message bytes.
 *
 * <p>This column is present in any {@link RecordTable
 * RecordTable} and serves for convenient record
 * {@link Serializer deserialization}.
 *
 * //TODO:2021-12-21:alex.tymchenko: review this paragraph.
 * <p>The column getter can be applied to an arbitrary message and is parameterized only to
 * enable usage along with message-specific table message-specific columns.
 */
public final class BytesColumn extends TableColumn {

    private static final String NAME = "bytes";

    public BytesColumn(JdbcColumnMapping mapping) {
        super(NAME, byte[].class, mapping);
    }

    /**
     * Returns the name of this column.
     *
     * <p>This method is designed for those wishing to use the columns
     * of this type in SQL expressions.
     */
    public static String bytesColumnName() {
        return NAME;
    }

    /**
     * Returns the name of this column.
     */
    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Type type() {
        return BYTE_ARRAY;
    }

    @Override
    public @Nullable Object valueIn(RecordWithColumns<?, ?> record) {
        return serialize(record.record());
    }
}
