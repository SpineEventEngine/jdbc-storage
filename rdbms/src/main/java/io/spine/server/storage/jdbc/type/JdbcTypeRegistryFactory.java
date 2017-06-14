/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import io.spine.server.entity.storage.ColumnTypeRegistry;

import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.booleanType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.integerType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.longType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.messageType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.stringType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.timestampType;

/**
 * A factory of the Jdbc-specific {@link ColumnTypeRegistry ColumnTypeRegistries}.
 *
 * @author Alexander Aleksandrov
 */
public final class JdbcTypeRegistryFactory {

    private JdbcTypeRegistryFactory() {
        // Prevent initialization of a utility class
    }

    private static final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>>
            DEFAULT_REGISTRY = ColumnTypeRegistry.<JdbcColumnType<?, ?>>newBuilder()
            .put(String.class, stringType())
            .put(Integer.class, integerType())
            .put(Long.class, longType())
            .put(Boolean.class, booleanType())
            .put(Timestamp.class, timestampType())
            .put(AbstractMessage.class, messageType())
            .build();

    public static ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> defaultInstance() {
        return DEFAULT_REGISTRY;
    }

}
