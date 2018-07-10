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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.testing.NullPointerTester;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmytro Dashenkov
 */
public class CloseablesShould {

    @Test
    @DisplayName("have private constructor")
    void havePrivateConstructor() {
        assertHasPrivateParameterlessCtor(Closeables.class);
    }

    @Test
    @DisplayName("pass null tolerance check")
    void passNullToleranceCheck() {
        new NullPointerTester()
                .testAllPublicStaticMethods(Closeables.class);
    }

    @SuppressWarnings("MethodWithMultipleLoops")
    // Two loops - one for data set up and one for checks
    @Test
    @DisplayName("close all passed instances")
    void closeAllPassedInstances() {
        final int count = 10;
        final Set<StatefulClosable> closeables = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            closeables.add(new StatefulClosable());
        }

        Closeables.closeAll(closeables);

        for (StatefulClosable sc : closeables) {
            assertTrue(sc.closed);
        }
    }

    @Test(expected = IllegalStateException.class)
    @DisplayName("throw Illegal state on failure")
    void throwIllegalStateOnFailure() {
        final AutoCloseable closeable = new FaultyClosable();
        Closeables.closeAll(singleton(closeable));
    }

    @Test
    @DisplayName("try to close all instances")
    void tryToCloseAllInstances() {
        final AutoCloseable faulty = new FaultyClosable();
        final StatefulClosable stateful = new StatefulClosable();

        final Collection<AutoCloseable> closeables =
                Lists.newArrayList(faulty, stateful); // Needs to be ordered
        boolean success;
        try {
            Closeables.closeAll(closeables);
            success = true;
        } catch (IllegalStateException e) {
            success = false;
        }

        assertFalse(success);
        assertTrue((stateful.closed));
    }

    @Test
    @DisplayName("throw exception with aggregating cause upon multiple failures")
    void throwExceptionWithAggregatingCauseUponMultipleFailures() {
        final Collection<AutoCloseable> closeables =
                Sets.<AutoCloseable>newHashSet(new FaultyClosable(),
                                               new FaultyClosable());
        try {
            Closeables.closeAll(closeables);
        } catch (IllegalStateException e) {
            final Throwable cause = e.getCause();
            assertThat(cause, instanceOf(MultipleExceptionsOnClose.class));
        }
    }

    private static class StatefulClosable implements AutoCloseable {

        private boolean closed = false;

        @Override
        public void close() throws Exception {
            assertFalse(closed);
            closed = true;
        }
    }

    private static class FaultyClosable implements AutoCloseable {

        @Override
        public void close() throws Exception {
            throw new RuntimeException("Fault!");
        }
    }
}
