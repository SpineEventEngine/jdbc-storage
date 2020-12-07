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

import java.util.ArrayList;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility class for working with {@link AutoCloseable} instances.
 */
final class Closeables {

    /**
     * Prevents instantiation of this utility class.
     */
    private Closeables() {
    }

    /**
     * Closes all of the passed {@linkplain AutoCloseable AutoCloseables}.
     *
     * <p>In case of an {@linkplain Exception} on the {@link AutoCloseable#close()} tries to close
     * all the rest instances.
     *
     * <p>The very first {@linkplain Exception} caught will be propagated in
     * an {@link IllegalStateException}.
     *
     * @param closeables
     *         instances to close
     * @throws IllegalStateException
     *         if {@linkplain AutoCloseable#close() close()} throws an {@link Exception}
     */
    static void closeAll(Iterable<? extends AutoCloseable> closeables) {
        checkNotNull(closeables);
        Collection<Exception> exceptions = new ArrayList<>();
        for (AutoCloseable closable : closeables) {
            try {
                closable.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        if (exceptions.isEmpty()) {
            return;
        }

        Throwable cause;
        if (exceptions.size() == 1) {
            cause = exceptions.iterator()
                              .next();
        } else {
            cause = new MultipleExceptionsOnClose(exceptions);
        }

        throw new IllegalStateException(cause);
    }
}
