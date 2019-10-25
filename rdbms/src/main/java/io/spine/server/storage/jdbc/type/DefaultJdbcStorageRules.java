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

package io.spine.server.storage.jdbc.type;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.core.Version;
import io.spine.json.Json;
import io.spine.server.entity.storage.AbstractStorageRules;
import io.spine.server.entity.storage.ColumnStorageRule;
import io.spine.server.storage.jdbc.Type;

import static io.spine.server.entity.storage.ColumnStorageRule.identity;
import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;

public class DefaultJdbcStorageRules
        extends AbstractStorageRules<Object>
        implements JdbcColumnStorageRules<Object> {

    @Override
    public Type typeOf(Class<?> clazz) {
        JdbcStorageRule<?, ?> persistenceStrategy =
                (JdbcStorageRule<?, ? >) of(clazz);
        Type type = persistenceStrategy.storeAs();
        return type;
    }

    @Override
    protected void
    setupCustomRules(ImmutableMap.Builder<Class<?>, ColumnStorageRule<?, ?>> builder) {
        builder.put(Timestamp.class, ofTimestamp());
        builder.put(Version.class, ofVersion());
    }

    @Override
    protected ColumnStorageRule<String, String> ofString() {
        return new JdbcStorageRule<>(identity(), STRING);
    }

    @Override
    protected ColumnStorageRule<Integer, Integer> ofInteger() {
        return new JdbcStorageRule<>(identity(), INT);
    }

    @Override
    protected ColumnStorageRule<Long, Long> ofLong() {
        return new JdbcStorageRule<>(identity(), LONG);
    }

    @Override
    protected ColumnStorageRule<Float, Float> ofFloat() {
        throw unsupportedType(Float.class);
    }

    @Override
    protected ColumnStorageRule<Double, Double> ofDouble() {
        throw unsupportedType(Double.class);
    }

    @Override
    protected ColumnStorageRule<Boolean, Boolean> ofBoolean() {
        return new JdbcStorageRule<>(identity(), BOOLEAN);
    }

    @Override
    protected ColumnStorageRule<ByteString, byte[]> ofByteString() {
        return new JdbcStorageRule<>(ByteString::toByteArray, BYTE_ARRAY);
    }

    @Override
    protected ColumnStorageRule<Enum<?>, Integer> ofEnum() {
        return new JdbcStorageRule<>(Enum::ordinal, INT);
    }

    @Override
    protected ColumnStorageRule<Message, String> ofMessage() {
        return new JdbcStorageRule<>(Json::toCompactJson, STRING);
    }

    @Override
    public ColumnStorageRule<?, ?> ofNull() {
        return identity();
    }

    private static JdbcStorageRule<Timestamp, Long> ofTimestamp() {
        return new JdbcStorageRule<>(Timestamps::toMillis, LONG);
    }

    private static JdbcStorageRule<Version, Integer> ofVersion() {
        return new JdbcStorageRule<>(Version::getNumber, INT);
    }
}
