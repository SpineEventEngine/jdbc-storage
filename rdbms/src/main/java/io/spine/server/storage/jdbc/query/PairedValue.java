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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A combination of two values for iterating over the SQL query results.
 *
 * <p>Nulls are not accepted as values for the usage convenience.
 *
 * @param <A>
 *         the first value type
 * @param <B>
 *         the second value type
 */
public final class PairedValue<A, B> {

    private final A aValue;
    private final B bValue;

    private PairedValue(A aValue, B bValue) {
        this.aValue = aValue;
        this.bValue = bValue;
    }

    /**
     * Creates a {@code PairedValue} instance for the two given values.
     */
    public static <A, B> PairedValue<A, B> of(A a, B b) {
        checkNotNull(a);
        checkNotNull(b);
        return new PairedValue<>(a, b);
    }

    public A aValue() {
        return aValue;
    }

    public B bValue() {
        return bValue;
    }
}
