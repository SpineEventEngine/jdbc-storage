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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.testing.NullPointerTester;
import io.spine.server.storage.jdbc.aggregate.given.CloseablesTestEnv.FaultyClosable;
import io.spine.server.storage.jdbc.aggregate.given.CloseablesTestEnv.StatefulClosable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Closeables utility should")
class CloseablesTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void haveUtilityConstructor() {
        assertHasPrivateParameterlessCtor(Closeables.class);
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .testAllPublicStaticMethods(Closeables.class);
    }

    @SuppressWarnings("MethodWithMultipleLoops")
    // Two loops - one for data set up and one for checks.
    @Test
    @DisplayName("close all passed instances")
    void closeAll() {
        int count = 10;
        Set<StatefulClosable> closeables = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            closeables.add(new StatefulClosable());
        }

        Closeables.closeAll(closeables);

        for (StatefulClosable sc : closeables) {
            assertTrue(sc.isClosed());
        }
    }

    @Test
    @DisplayName("throw ISE when closing fails")
    void throwOnCloseFailure() {
        AutoCloseable closeable = new FaultyClosable();
        assertThrows(IllegalStateException.class, () -> Closeables.closeAll(singleton(closeable)));
    }

    @Test
    @DisplayName("try to close all instances")
    void tryToCloseAll() {
        AutoCloseable faulty = new FaultyClosable();
        StatefulClosable stateful = new StatefulClosable();

        // Needs to be ordered.
        Collection<AutoCloseable> closeables = Lists.newArrayList(faulty, stateful);
        boolean success;
        try {
            Closeables.closeAll(closeables);
            success = true;
        } catch (IllegalStateException e) {
            success = false;
        }

        assertFalse(success);
        assertTrue(stateful.isClosed());
    }

    @Test
    @DisplayName("throw exception with aggregating cause upon multiple failures")
    void throwAggregatedFailures() {
        Collection<AutoCloseable> closeables = Sets.newHashSet(new FaultyClosable(),
                                                               new FaultyClosable());
        try {
            Closeables.closeAll(closeables);
        } catch (IllegalStateException e) {
            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(MultipleExceptionsOnClose.class));
        }
    }
}
