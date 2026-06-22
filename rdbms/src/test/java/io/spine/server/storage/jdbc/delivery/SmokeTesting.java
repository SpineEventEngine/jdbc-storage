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

package io.spine.server.storage.jdbc.delivery;

/**
 * Constants shared by the JDBC-backed smoke tests of this package.
 *
 * @see JdbcDeliverySmokeTest
 * @see JdbcCatchUpSmokeTest
 */
final class SmokeTesting {

    /**
     * The reason a smoke test case is {@link org.junit.jupiter.api.Disabled @Disabled}.
     *
     * <p>The disabled cases inherit extremely slow scenarios of the original test suites.
     * Only a small portion of those suites is launched as a smoke check; the remaining
     * cases are disabled to keep the build reasonably fast. See the documentation of the
     * referencing test class for details.
     */
    static final String DISABLED_REASON =
            "Too slow for the smoke suite. Only a small portion of the inherited tests" +
            " is launched; see the class documentation.";

    /** Prevents instantiation of this constants holder. */
    private SmokeTesting() {
    }
}
