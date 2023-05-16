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

package io.spine.server.storage.jdbc.type;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.core.Version;
import io.spine.json.Json;
import io.spine.server.entity.storage.AbstractColumnMapping;
import io.spine.server.entity.storage.ColumnTypeMapping;
import io.spine.server.storage.jdbc.Type;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.entity.storage.ColumnTypeMapping.identity;
import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;

/**
 * The default column mapping for all storages.
 *
 * <p>Users may extend this class to add own custom mapping for some of the stored types.
 */
public class DefaultJdbcColumnMapping
        extends AbstractColumnMapping<Object>
        implements JdbcColumnMapping<Object> {

    @Override
    public Type typeOf(Class<?> columnType) {
        checkNotNull(columnType);
        JdbcColumnTypeMapping<?, ?> typeMapping =
                (JdbcColumnTypeMapping<?, ? >) of(columnType);
        Type type = typeMapping.storeAs();
        return type;
    }

    @Override
    protected void
    setupCustomMapping(ImmutableMap.Builder<Class<?>, ColumnTypeMapping<?, ?>> builder) {
        builder.put(Timestamp.class, ofTimestamp());
        builder.put(Version.class, ofVersion());
    }

    @Override
    protected ColumnTypeMapping<String, String> ofString() {
        return new JdbcColumnTypeMapping<>(identity(), STRING);
    }

    @Override
    protected ColumnTypeMapping<Integer, Integer> ofInteger() {
        return new JdbcColumnTypeMapping<>(identity(), INT);
    }

    @Override
    protected ColumnTypeMapping<Long, Long> ofLong() {
        return new JdbcColumnTypeMapping<>(identity(), LONG);
    }

    @Override
    protected ColumnTypeMapping<Float, Float> ofFloat() {
        throw unsupportedType(Float.class);
    }

    @Override
    protected ColumnTypeMapping<Double, Double> ofDouble() {
        throw unsupportedType(Double.class);
    }

    @Override
    protected ColumnTypeMapping<Boolean, Boolean> ofBoolean() {
        return new JdbcColumnTypeMapping<>(identity(), BOOLEAN);
    }

    @Override
    protected ColumnTypeMapping<ByteString, byte[]> ofByteString() {
        return new JdbcColumnTypeMapping<>(ByteString::toByteArray, BYTE_ARRAY);
    }

    @Override
    protected ColumnTypeMapping<Enum<?>, Integer> ofEnum() {
        return new JdbcColumnTypeMapping<>(Enum::ordinal, INT);
    }

    @Override
    protected ColumnTypeMapping<Message, String> ofMessage() {
        return new JdbcColumnTypeMapping<>(Json::toCompactJson, STRING);
    }

    @Override
    public ColumnTypeMapping<?, ?> ofNull() {
        return identity();
    }

    private static JdbcColumnTypeMapping<Timestamp, Long> ofTimestamp() {
        return new JdbcColumnTypeMapping<>(Timestamps::toNanos, LONG);
    }

    private static JdbcColumnTypeMapping<Version, Integer> ofVersion() {
        return new JdbcColumnTypeMapping<>(Version::getNumber, INT);
    }
}
