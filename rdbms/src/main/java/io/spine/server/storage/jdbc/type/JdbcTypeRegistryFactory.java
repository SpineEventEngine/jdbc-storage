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
import io.spine.core.Version;
import io.spine.server.entity.storage.ColumnTypeRegistry;

import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.booleanType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.integerType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.longType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.messageType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.stringType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.timestampType;
import static io.spine.server.storage.jdbc.type.JdbcColumnTypes.versionType;

/**
 * A factory of the Jdbc-specific {@link ColumnTypeRegistry ColumnTypeRegistries}.
 *
 * @author Alexander Aleksandrov
 */
public final class JdbcTypeRegistryFactory {

    private JdbcTypeRegistryFactory() {
        // Prevent initialization of a utility class
    }

    private static final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
            DEFAULT_REGISTRY =
            ColumnTypeRegistry.<JdbcColumnType<? super Object, ? super Object>>newBuilder()
                              .put(String.class, stringType())
                              .put(Integer.class, integerType())
                              .put(Long.class, longType())
                              .put(Boolean.class, booleanType())
                              .put(Version.class, versionType())
                              .put(Timestamp.class, timestampType())
                              .put(AbstractMessage.class, messageType())
                              .build();

    /**
     * Retrieves a default
     * {@link ColumnTypeRegistry ColumnTypeRegistry&lt;? extends JdbcColumnType&gt;}.
     *
     * The returned registry contains the
     * {@linkplain io.spine.server.entity.storage.ColumnType column types} declarations for:
     * <ul>
     *     <li>{@code String}
     *     <li>{@code Integer}
     *     <li>{@code Long}
     *     <li>{@code Boolean}
     *     <li>{@link Timestamp} stored as {@link java.sql.Timestamp Timestamp}
     *     <li>{@link AbstractMessage Message} stored as a {@code String} retrieved form a
     *     {@link io.spine.json.Json#toCompactJson}
     *     <li>{@link Version} stored as an {@code int} version number
     * </ul>
     *
     * @return the default {@code ColumnTypeRegistry} for storing the Entity Columns in Jdbc storage
     */
    public static ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
    defaultInstance() {
        return DEFAULT_REGISTRY;
    }

    /**
     * Retrieves a builder with all the {@linkplain #defaultInstance() predefined values} set.
     */
    public static ColumnTypeRegistry.Builder<? extends JdbcColumnType<? super Object, ? super Object>>
    predefinedValuesAnd() {
        return ColumnTypeRegistry.newBuilder(DEFAULT_REGISTRY);
    }

}
