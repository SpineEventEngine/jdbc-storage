/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.spine.base.Identifier.newUuid;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("Parameters should")
class ParametersTest {

    private static final String ID = newUuid();
    private static final Parameter PARAMETER = Parameter.of(new Object());

    @Test
    @DisplayName("check identifier uniqueness for single parameter")
    void checkIdUniqueForSingle() {
        assertThrows(IllegalArgumentException.class,
                     () -> Parameters.newBuilder()
                                     .addParameter(ID, PARAMETER)
                                     .addParameter(ID, PARAMETER));
    }

    @Test
    @DisplayName("check identifier uniqueness for multiple parameters")
    void checkIdUniqueForMultiple() {
        Parameters.Builder commonParameters = Parameters.newBuilder()
                                                        .addParameter(ID, PARAMETER);
        Parameters buildedCommonParameters = commonParameters.build();

        assertThrows(IllegalArgumentException.class,
                     () -> commonParameters.addParameters(buildedCommonParameters));
    }

    @Test
    @DisplayName("not allow modify identifiers")
    void notAllowModifyId() {
        Parameters parameters = Parameters.empty();
        Set<String> identifiers = parameters.getIdentifiers();
        String newIdentifier = newUuid();
        assertThrows(UnsupportedOperationException.class, () -> identifiers.add(newIdentifier));
    }
}
