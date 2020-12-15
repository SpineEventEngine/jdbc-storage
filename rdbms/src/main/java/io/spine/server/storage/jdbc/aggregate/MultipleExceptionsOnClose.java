/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.base.Throwables;

import java.util.ArrayList;
import java.util.Collection;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;

/**
 * An {@link Exception} telling that there were <b>multiple</b> exceptions while trying to
 * {@link Closeables#closeAll(Iterable)} close} multiple closeables.
 */
final class MultipleExceptionsOnClose extends Throwable {

    private static final long serialVersionUID = 0L;
    private final Collection<Exception> exceptions;

    MultipleExceptionsOnClose(Collection<Exception> exceptions) {
        super(format("%s fatal exceptions.", exceptions.size()));
        this.exceptions = new ArrayList<>(exceptions);
    }

    /**
     * Describes all the given {@link Exception exceptions}.
     *
     * @return the description for all the exceptions
     */
    @Override
    public String getMessage() {
        @SuppressWarnings("StringBufferWithoutInitialCapacity") /* Stacktrace size is unknown. */
                StringBuilder builder = new StringBuilder();
        for (Exception exception : exceptions) {
            String stackTrace = Throwables.getStackTraceAsString(exception);
            builder.append(stackTrace)
                   .append(lineSeparator());
        }
        String result = builder.toString();
        return result;
    }
}
