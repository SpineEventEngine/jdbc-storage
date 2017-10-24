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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * A parameters for a SQL query.
 *
 * <p>The parameters have {@linkplain #getIdentifiers() identifiers} to distinguish a place
 * in a query where a parameter should be inserted.
 *
 * @author Dmytro Grankin
 */
public class IdentifiedParameters {

    private final Map<String, Object> parameters;

    private IdentifiedParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    /**
     * Obtains a set of parameter identifiers.
     *
     * @return parameter identifiers
     */
    Set<String> getIdentifiers() {
        return parameters.keySet();
    }

    /**
     * Obtains a raw value of the parameter by the specified identifier.
     *
     * @param identifier the parameter identifier
     * @return a raw parameter value
     * @throws IllegalArgumentException if there is no parameters with the specified identifier
     */
    Object getValue(String identifier) {
        checkArgument(parameters.containsKey(identifier));
        return parameters.get(identifier);
    }

    static IdentifiedParameters empty() {
        return new IdentifiedParameters(Collections.<String, Object>emptyMap());
    }

    static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final ImmutableMap.Builder<String, Object> parameters = ImmutableMap.builder();

        private Builder() {
            // Prevent direct instantiation of this class.
        }

        public Builder addParameter(String identifier, Object value) {
            checkArgument(!isNullOrEmpty(identifier));

            parameters.put(identifier, value);
            return this;
        }

        public Builder addParameters(IdentifiedParameters identifiedParameters) {
            parameters.putAll(identifiedParameters.parameters);
            return this;
        }

        IdentifiedParameters build() {
            return new IdentifiedParameters(parameters.build());
        }
    }
}
