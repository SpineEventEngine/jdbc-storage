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

package io.spine.server.storage.jdbc.query;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.disjoint;

/**
 * A {@linkplain Parameter query parameters} with identifiers.
 *
 * <p>{@linkplain #getIdentifiers() Identifiers} allows to distinguish a place
 * in a query where a parameter should be inserted.
 *
 * <p>This class allows to encapsulate setting of parameters to a concrete implementation.
 *
 * @author Dmytro Grankin
 */
public final class Parameters {

    private final ImmutableMap<String, Parameter> parameters;

    private Parameters(Builder builder) {
        this.parameters = ImmutableMap.copyOf(builder.parameters);
    }

    /**
     * Obtains a set of parameter identifiers.
     *
     * @return parameter identifiers
     */
    public Set<String> getIdentifiers() {
        // It's OK for an immutable map to return the key set directly.
        return parameters.keySet();
    }

    /**
     * Obtains a {@link Parameter} by the specified identifier.
     *
     * @param identifier a parameter identifier
     * @return the query parameter
     * @throws IllegalArgumentException if there is no parameters with the specified identifier
     */
    public Parameter getParameter(String identifier) {
        checkArgument(parameters.containsKey(identifier));
        final Parameter value = parameters.get(identifier);
        return value;
    }

    /**
     * Creates empty {@code Parameters}.
     *
     * @return empty parameters
     */
    public static Parameters empty() {
        return newBuilder().build();
    }

    /**
     * Creates a builder for {@code Parameters}.
     *
     * @return the builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for {@link Parameters}.
     *
     * <p>The builder doesn't allow to create {@link Parameters} with duplicated identifiers.
     */
    public static class Builder {

        private final Map<String, Parameter> parameters = newHashMap();

        private Builder() {
            // Prevent direct instantiation of this class.
        }

        /**
         * Adds a parameters with the specified identifier.
         *
         * @param identifier the identifier for a parameter
         * @param parameter the {@link Parameter} to add
         */
        public Builder addParameter(String identifier, Parameter parameter) {
            checkNotNull(identifier);
            checkArgument(!parameters.containsKey(identifier));
            parameters.put(identifier, parameter);
            return this;
        }

        /**
         * Adds {@linkplain Parameters parameters with identifiers} to the builder.
         *
         * @param otherParameters the parameters to add
         * @throws IllegalArgumentException if duplicated identifiers were found
         */
        public Builder addParameters(Parameters otherParameters) {
            checkArgument(disjoint(parameters.keySet(), otherParameters.parameters.keySet()));
            parameters.putAll(otherParameters.parameters);
            return this;
        }

        /**
         * @return the assembled {@code Parameters}
         */
        public Parameters build() {
            return new Parameters(this);
        }
    }
}
