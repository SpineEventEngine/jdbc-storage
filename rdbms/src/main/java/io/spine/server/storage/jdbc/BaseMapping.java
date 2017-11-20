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

import java.util.EnumMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;

/**
 * A base implementation of {@link TypeMapping}.
 *
 * @author Dmytro Grankin
 */
public final class BaseMapping implements TypeMapping {

    private final Map<Type, String> mappedTypes;

    private BaseMapping(Map<Type, String> mappedTypes) {
        this.mappedTypes = mappedTypes;
    }

    /**
     * Obtains the base builder for mappings.
     *
     * <p>All the types are mapped as follows:
     * <ul>
     *     <li>{@code Type.BYTE_ARRAY} - {@code BLOB}</li>
     *     <li>{@code Type.INT} - {@code INT}</li>
     *     <li>{@code Type.LONG} - {@code BIGINT}</li>
     *     <li>{@code Type.STRING_255} - {@code VARCHAR(255)}</li>
     *     <li>{@code Type.STRING} - {@code TEXT}</li>
     *     <li>{@code Type.BOOLEAN} - {@code BOOLEAN}</li>
     * </ul>
     *
     * <p>{@linkplain Builder#add(Type, String) Override} the type name
     * if it doesn't match a database.
     *
     * @return the builder with all types
     */
    public static Builder baseBuilder() {
        final Builder baseMapping = new Builder().add(BYTE_ARRAY, "BLOB")
                                                 .add(INT, "INT")
                                                 .add(LONG, "BIGINT")
                                                 .add(STRING_255, "VARCHAR(255)")
                                                 .add(STRING, "TEXT")
                                                 .add(BOOLEAN, "BOOLEAN");
        return baseMapping;
    }

    @Override
    public String getTypeName(Type type) {
        checkState(mappedTypes.containsKey(type),
                   "The type mapping doesn't define name for %s type.", type);
        final String name = mappedTypes.get(type);
        return name;
    }

    /**
     * A builder for {@link BaseMapping}.
     */
    public static class Builder {

        private final Map<Type, String> mappedTypes = new EnumMap<>(Type.class);

        /**
         * Adds a mapping for the specified type.
         *
         * <p>Overrides the name of the type if it is already specified.
         *
         * @param type the type for the mapping
         * @param name the custom name for the type
         * @return the builder instance
         */
        public Builder add(Type type, String name) {
            checkNotNull(type);
            checkArgument(!isNullOrEmpty(name));
            mappedTypes.put(type, name);
            return this;
        }

        /**
         * Creates {@link BaseMapping} for the builder.
         *
         * @return a new type mapping
         * @throws IllegalStateException if not all the {@linkplain Type types} were mapped
         */
        public BaseMapping build() {
            final int typesCount = Type.values().length;
            checkState(mappedTypes.size() == typesCount,
                       "A mapping should contain names for all types (%s), " +
                       "but only (%s) types were mapped.", typesCount, mappedTypes.size());
            return new BaseMapping(mappedTypes);
        }
    }

}
