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

package io.spine.server.storage.jdbc;

import com.google.common.collect.ImmutableMap;

import java.util.EnumMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * A {@link Type}-to-name mapping.
 *
 * <p>A data type may have different names in different databases.
 * E.g. for binary data {@code BLOB} is used in MySQL, but in PostgreSQL it's {@code BYTEA}.
 *
 * <p>This class provides a flexible way to point out
 * database specific names of {@linkplain Type types}.
 *
 * <p>There are standard {@linkplain TypeMappings mappings}.
 *
 * @author Dmytro Grankin
 */
public final class TypeMapping {

    private final Map<Type, String> mappedTypes;

    private TypeMapping(Map<Type, String> mappedTypes) {
        this.mappedTypes = mappedTypes;
    }

    /**
     * Obtains the name of the specified {@link Type}.
     *
     * @param type the type to get the name
     * @return the type name
     * @throws IllegalStateException if the name for the specified type is not defined
     */
    public String getTypeName(Type type) {
        checkState(mappedTypes.containsKey(type),
                   "The type mapping doesn't define name for %s type.", type);
        final String name = mappedTypes.get(type);
        return name;
    }

    /**
     * Creates a new instance of the {@link TypeMapping} builder.
     *
     * <p>All the {@link Type types} should be mapped.
     *
     * <p>A MySQL-specific mapping may look as follows:
     * <ul>
     *     <li>{@code Type.BYTE_ARRAY} - BLOB</li>
     *     <li>{@code Type.INT} - INT</li>
     *     <li>{@code Type.LONG} - BIGINT</li>
     *     <li>{@code Type.STRING_255} - VARCHAR(255)</li>
     *     <li>{@code Type.STRING} - TEXT</li>
     *     <li>{@code Type.BOOLEAN} - BOOLEAN</li>
     * </ul>
     *
     * @return the new builder instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for {@link TypeMapping}.
     */
    public static class Builder {

        private final ImmutableMap.Builder<Type, String> types = ImmutableMap.builder();

        private Builder() {
            // Prevent direct instantiation of this class.
        }

        /**
         * Adds a mapping for the specified type.
         *
         * @param type the type for the mapping
         * @param name the custom name for the type
         * @return the builder instance
         */
        public Builder add(Type type, String name) {
            checkNotNull(type);
            checkArgument(!isNullOrEmpty(name));
            types.put(type, name);
            return this;
        }

        /**
         * Creates {@link TypeMapping} for the builder.
         *
         * @return a new type mapping
         * @throws IllegalStateException if not all of the {@linkplain Type types} were mapped
         */
        public TypeMapping build() {
            final Map<Type, String> mappedTypes = new EnumMap<>(types.build());
            final int typesCount = Type.values().length;
            checkState(mappedTypes.size() == typesCount,
                       "A mapping should contain names for all types (%s), " +
                       "but only (%s) types were mapped.", typesCount, mappedTypes.size());
            return new TypeMapping(mappedTypes);
        }
    }
}
