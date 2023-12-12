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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.annotation.SPI;
import io.spine.core.Version;
import io.spine.server.storage.AbstractColumnMapping;
import io.spine.server.storage.ColumnTypeMapping;
import io.spine.server.storage.jdbc.Type;
import io.spine.type.Json;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.ColumnTypeMapping.identity;
import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;

/**
 * Scheme of relations between Java types of the values stored in record columns,
 * and storage-specific types.
 *
 * <p>Also defines the RDBMS-level types for the table columns.
 *
 * <p>Users may extend this type to add own custom mapping for some of the stored types.
 */
@SPI
public class JdbcColumnMapping extends AbstractColumnMapping<Object> {

    private static final Map<Class<?>, ColumnTypeMapping<?, ?>> defaults
            = ImmutableMap.of(Timestamp.class, ofTimestamp(),
                              Version.class, ofVersion());

    /**
     * Returns an RDBMS-specific type for the particular type of values
     * stored in a column.
     *
     * <p>This method differs from {@link #of(Class) of(Class)},
     * as it returns not a persistence strategy for a column,
     * but a type to use with RDBMS. Different implementations
     * may even choose to return different types,
     * depending on the version of underlying DB engine.
     */
    public Type typeOf(Class<?> columnType) {
        checkNotNull(columnType);
        var typeMapping = (JdbcColumnTypeMapping<?, ?>) of(columnType);
        var type = typeMapping.storeAs();
        return type;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Merges the default column mapping rules with those provided by SPI users.
     * In case there are duplicate mappings for some column type, the value provided
     * by SPI users is used.
     *
     * @apiNote This method is made {@code final}, as it is designed
     *         to use {@code ImmutableMap.Builder}, which does not allow to override values.
     *         Therefore, it is not possible for SPI users to provide their own mapping rules
     *         for types such as {@code Timestamp}, for which this class already has
     *         a default mapping. SPI users should override
     *         {@link #customRules() JdbcColumnMapping.customRules()} instead.
     */
    @Override
    protected final void
    setupCustomMapping(ImmutableMap.Builder<Class<?>, ColumnTypeMapping<?, ?>> builder) {
        Map<Class<?>, ColumnTypeMapping<?, ?>> merged = new HashMap<>();
        var custom = customRules();
        merged.putAll(defaults);
        merged.putAll(custom);
        builder.putAll(merged);
    }

    /**
     * Returns the custom column mapping rules.
     *
     * <p>This method is designed for SPI users in order to be able to re-define
     * and-or append their custom mapping. As by default, {@code DefaultJdbcColumnMapping}
     * provides rules for {@link Timestamp} and {@link Version}, SPI users may
     * choose to either override these defaults by returning their own mapping for these types,
     * or supply even more mapping rules.
     *
     * <p>By default, this method returns an empty map.
     *
     * @return custom column mappings, per Java class of column
     */
    @SPI
    protected ImmutableMap<Class<?>, JdbcColumnTypeMapping<?, ?>> customRules() {
        return ImmutableMap.of();
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

    /**
     * Returns the default mapping from {@link Timestamp} to {@link Long}.
     */
    protected static JdbcColumnTypeMapping<Timestamp, Long> ofTimestamp() {
        return new JdbcColumnTypeMapping<>(Timestamps::toNanos, LONG);
    }

    /**
     * Returns the default mapping from {@link Version} to {@link Integer}.
     */
    protected static JdbcColumnTypeMapping<Version, Integer> ofVersion() {
        return new JdbcColumnTypeMapping<>(Version::getNumber, INT);
    }
}
