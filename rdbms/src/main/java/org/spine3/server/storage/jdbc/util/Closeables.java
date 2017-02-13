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

package org.spine3.server.storage.jdbc.util;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility class for working with {@link AutoCloseable} instances.
 *
 * @author Dmytro Dashenkov.
 */
public class Closeables {

    private Closeables() {
    }

    /**
     * Closes all of the passed {@linkplain AutoCloseable AutoCloseables}.
     *
     * <p>In case of an {@linkplain Exception} on the {@link AutoCloseable#close()} tries to close
     * all the rest instances.
     *
     * <p>The very first {@linkplain Exception} caught will be propagated in an {@link IllegalStateException}.
     *
     * @param closeables instances to close
     * @throws IllegalStateException if {@linkplain AutoCloseable#close() close()} throws an {@link Exception}
     */
    public static void closeAll(Iterable<? extends AutoCloseable> closeables) {
        checkNotNull(closeables);
        Exception exception = null;
        AutoCloseable faultyInstance = null;
        for (AutoCloseable closable : closeables) {
            try {
                closable.close();
            } catch (Exception e) {
                if (exception == null) {
                    exception = e;
                    faultyInstance = closable;
                }
            }
        }

        if (exception != null) {
            throw new IllegalStateException(
                    String.format("Exception trying to close %s.", faultyInstance),
                    exception);
        }
    }
}
