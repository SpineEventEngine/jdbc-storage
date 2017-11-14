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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.spine.server.storage.jdbc.Sql.Type;

import java.util.EnumMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * A custom {@link Type} mapping.
 *
 * <p>This class allows to specify custom SQL type names for the standard types.
 *
 * @author Dmytro Grankin
 */
public class TypeMapping {

    private final Map<Type, String> mappedTypes;

    private TypeMapping(Builder builder) {
        mappedTypes = new EnumMap<>(builder.types.build());
    }

    public Optional<String> getTypeName(Type type) {
        checkNotNull(type);
        final String customName = mappedTypes.get(type);
        return Optional.fromNullable(customName);
    }

    /**
     * Creates a new instance of the {@link TypeMapping} builder.
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
         */
        public TypeMapping build() {
            return new TypeMapping(this);
        }
    }
}
