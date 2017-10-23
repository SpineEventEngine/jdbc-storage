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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * A list of named parameters for a SQL query.
 *
 * @author Dmytro Grankin
 */
class NamedParameters {

    private final Map<String, Object> parameters;

    private NamedParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    /**
     * Obtains a set of parameter names.
     *
     * @return parameter names
     */
    Set<String> getNames() {
        return parameters.keySet();
    }

    /**
     * Obtains a raw value of the parameter by the specified name.
     *
     * @param name the parameter name
     * @return a raw parameter value
     * @throws IllegalArgumentException if there is no parameters with the specified name
     */
    Object getValue(String name) {
        checkArgument(parameters.containsKey(name));
        return parameters.get(name);
    }

    static NamedParameters empty() {
        return new NamedParameters(Collections.<String, Object>emptyMap());
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder {

        private final ImmutableMap.Builder<String, Object> parameters = ImmutableMap.builder();

        private Builder() {
            // Prevent direct instantiation of this class.
        }

        Builder addParameter(String name, Object value) {
            checkArgument(!isNullOrEmpty(name));
            checkNotNull(value);

            parameters.put(name, value);
            return this;
        }

        Builder addParameters(NamedParameters namedParameters) {
            parameters.putAll(namedParameters.parameters);
            return this;
        }

        NamedParameters build() {
            return new NamedParameters(parameters.build());
        }
    }
}
