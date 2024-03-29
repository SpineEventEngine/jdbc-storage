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

package io.spine.server.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.spine.type.TypeName;

import java.util.EnumMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.server.storage.jdbc.Type.STRING_512;
import static java.util.Objects.requireNonNull;

/**
 * A builder for {@linkplain TypeMapping type mappings}.
 */
public final class TypeMappingBuilder {

    private final Map<Type, TypeName> mappedTypes = new EnumMap<>(Type.class);

    /**
     * Creates a new builder.
     *
     * <p>Exposed just for tests. Otherwise, would be {@code private}.
     */
    @VisibleForTesting
    TypeMappingBuilder() {
    }

    /**
     * Obtains the basic builder for mappings.
     *
     * <p>All the types are mapped in the builder as follows:
     * <ul>
     *     <li>{@code Type.BYTE_ARRAY} - {@code BLOB}</li>
     *     <li>{@code Type.INT} - {@code INT}</li>
     *     <li>{@code Type.LONG} - {@code BIGINT}</li>
     *     <li>{@code Type.STRING_255} - {@code VARCHAR(255)}</li>
     *     <li>{@code Type.STRING_512} - {@code VARCHAR(512)}</li>
     *     <li>{@code Type.STRING} - {@code TEXT}</li>
     *     <li>{@code Type.BOOLEAN} - {@code BOOLEAN}</li>
     * </ul>
     *
     * <p>If the mapping provided by the builder doesn't match a database, it can be
     * {@linkplain TypeMappingBuilder#add(Type, String) overridden} as follows:
     *
     * <pre>{@code
     * TypeMapping mapping = mappingBuilder()
     *                          .add(Type.INT, "INT4")
     *                          .add(Type.LONG, "INT8")
     *                          .build();
     * }</pre>
     *
     * @return the builder containing names for all types
     */
    public static TypeMappingBuilder mappingBuilder() {
        var builder = new TypeMappingBuilder()
                .add(BYTE_ARRAY, "BLOB")
                .add(INT, "INT")
                .add(LONG, "BIGINT")
                .add(STRING_255, "VARCHAR(255)")
                .add(STRING_512, "VARCHAR(512)")
                .add(STRING, "TEXT")
                .add(BOOLEAN, "BOOLEAN");
        return builder;
    }

    /**
     * Adds a mapping for the specified type.
     *
     * <p>Overrides the name of the type if it is already specified.
     *
     * @param type
     *         the type for the mapping
     * @param name
     *         the custom name for the type
     * @return the builder instance
     */
    public TypeMappingBuilder add(Type type, String name) {
        checkNotNull(type);
        mappedTypes.put(type, TypeName.of(name));
        return this;
    }

    /**
     * Creates {@link TypeMapping} from the builder.
     *
     * @return a new type mapping
     * @throws IllegalStateException
     *         if not all the {@linkplain Type types} were mapped
     */
    public TypeMapping build() {
        var typeCount = Type.values().length;
        checkState(mappedTypes.size() == typeCount,
                   "A mapping should contain names for all types (%s), " +
                           "but only (%s) types were mapped.", typeCount, mappedTypes.size());
        return new TypeMappingImpl(mappedTypes);
    }

    /**
     * A {@link TypeMapping}, which is created by the {@link TypeMappingBuilder}.
     */
    @Immutable
    private static final class TypeMappingImpl implements TypeMapping {

        private final ImmutableMap<Type, TypeName> mappedTypes;

        private TypeMappingImpl(Map<Type, TypeName> mappedTypes) {
            this.mappedTypes = ImmutableMap.copyOf(mappedTypes);
        }

        @Override
        public TypeName typeNameFor(Type type) {
            checkState(mappedTypes.containsKey(type),
                       "The type mapping does not define a name for %s type.", type);
            var typeName = mappedTypes.get(type);
            return requireNonNull(typeName);
        }
    }
}
