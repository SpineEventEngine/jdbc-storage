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
import io.spine.server.entity.storage.AbstractTypeRegistry;
import io.spine.server.entity.storage.PersistenceStrategy;
import io.spine.server.entity.storage.PersistenceStrategyOfNull;
import io.spine.server.storage.jdbc.Type;

import java.util.function.Supplier;

import static io.spine.server.entity.storage.PersistenceStrategy.identity;
import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;

public class JdbcTypeRegistry extends AbstractTypeRegistry<Object> {

    public final Type typeOf(Class<?> clazz) {
        JdbcPersistenceStrategy<?, ?> persistenceStrategy =
                (JdbcPersistenceStrategy<?, ? >) persistenceStrategyOf(clazz);
        Type type = persistenceStrategy.storeAs();
        return type;
    }

    @Override
    protected void setupCustomStrategies(
            ImmutableMap.Builder<Class<?>, Supplier<PersistenceStrategy<?, ?>>> builder) {

        builder.put(Timestamp.class, JdbcTypeRegistry::timestampPersistenceStrategy);
        builder.put(Version.class, JdbcTypeRegistry::versionPersistenceStrategy);
    }

    @Override
    protected PersistenceStrategy<String, String> stringPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(identity(), STRING);
    }

    @Override
    protected PersistenceStrategy<Integer, Integer> integerPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(identity(), INT);
    }

    @Override
    protected PersistenceStrategy<Long, Long> longPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(identity(), LONG);
    }

    @Override
    protected PersistenceStrategy<Float, Float> floatPersistenceStrategy() {
        throw  unsupportedType(Float.class);
    }

    @Override
    protected PersistenceStrategy<Double, Double> doublePersistenceStrategy() {
        throw unsupportedType(Double.class);
    }

    @Override
    protected PersistenceStrategy<Boolean, Boolean> booleanPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(identity(), BOOLEAN);
    }

    @Override
    protected PersistenceStrategy<ByteString, byte[]> byteStringPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(ByteString::toByteArray, BYTE_ARRAY);
    }

    @Override
    protected PersistenceStrategy<Enum<?>, Integer> enumPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(Enum::ordinal, INT);
    }

    @Override
    protected PersistenceStrategy<Message, String> messagePersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(Json::toCompactJson, STRING);
    }

    @Override
    public PersistenceStrategyOfNull<Object> persistenceStrategyOfNull() {
        return () -> null;
    }

    private static JdbcPersistenceStrategy<Timestamp, Long> timestampPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(Timestamps::toMillis, LONG);
    }

    private static JdbcPersistenceStrategy<Version, Integer> versionPersistenceStrategy() {
        return new JdbcPersistenceStrategy<>(Version::getNumber, INT);
    }
}
