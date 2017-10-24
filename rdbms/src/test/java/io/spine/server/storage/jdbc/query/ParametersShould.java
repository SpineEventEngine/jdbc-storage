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

import org.junit.Test;

/**
 * @author Dmytro Grankin
 */
public class ParametersShould {

    private static final int ID = 1;
    private static final Object VALUE = new Object();

    @Test(expected = IllegalArgumentException.class)
    public void check_identifiers_uniqueness_for_single_parameter() {
        Parameters.newBuilder()
                  .addParameter(ID, VALUE)
                  .addParameter(ID, VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void check_identifiers_uniqueness_for_multiple_parameters() {
        final Parameters.Builder commonParameters = Parameters.newBuilder()
                                                              .addParameter(ID, VALUE);
        final Parameters buildedCommonParameters = commonParameters.build();
        commonParameters.addParameters(buildedCommonParameters);
    }
}
