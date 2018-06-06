/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import org.junit.Test;

import java.util.Set;

import static io.spine.base.Identifier.newUuid;

/**
 * @author Dmytro Grankin
 */
public class ParametersShould {

    private static final String ID = newUuid();
    private static final Parameter PARAMETER = Parameter.of(new Object());

    @Test(expected = IllegalArgumentException.class)
    public void check_identifiers_uniqueness_for_single_parameter() {
        Parameters.newBuilder()
                  .addParameter(ID, PARAMETER)
                  .addParameter(ID, PARAMETER);
    }

    @Test(expected = IllegalArgumentException.class)
    public void check_identifiers_uniqueness_for_multiple_parameters() {
        final Parameters.Builder commonParameters = Parameters.newBuilder()
                                                              .addParameter(ID, PARAMETER);
        final Parameters buildedCommonParameters = commonParameters.build();
        commonParameters.addParameters(buildedCommonParameters);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void not_allow_modify_identifiers() {
        final Parameters parameters = Parameters.empty();
        final Set<String> identifiers = parameters.getIdentifiers();
        final String newIdentifier = newUuid();
        identifiers.add(newIdentifier);
    }
}
