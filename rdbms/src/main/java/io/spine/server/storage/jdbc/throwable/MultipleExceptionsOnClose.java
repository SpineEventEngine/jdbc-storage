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

package io.spine.server.storage.jdbc.throwable;

import com.google.common.base.Throwables;
import io.spine.server.storage.jdbc.util.Closeables;

import java.util.Collection;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;

/**
 * An {@link Exception} telling that there were <b>multiple</b> exceptions while trying to
 * {@link Closeables#closeAll(Iterable)} close} multiple
 * closeables.
 *
 * @author Dmytro Dashenkov
 */
public class MultipleExceptionsOnClose extends Throwable {

    private static final long serialVersionUID = -0L;
    private final Collection<Exception> exceptions;

    public MultipleExceptionsOnClose(Collection<Exception> exceptions) {
        super(format("%s fatal exceptions.", exceptions.size()));
        this.exceptions = exceptions;
    }

    /**
     * Describes all the given {@link Exception exceptions}.
     *
     * @return the description for all the exceptions
     */
    @Override
    public String toString() {
        @SuppressWarnings("StringBufferWithoutInitialCapacity")
        // We don't know the size of the stacktrace
        final StringBuilder builder = new StringBuilder();
        final String selfInfo = super.toString();
        builder.append(selfInfo)
               .append(lineSeparator());
        for (Exception exception : exceptions) {
            final String stackTrace = Throwables.getStackTraceAsString(exception);
            builder.append(stackTrace)
                   .append(lineSeparator());
        }
        final String result = builder.toString();
        return result;
    }
}
