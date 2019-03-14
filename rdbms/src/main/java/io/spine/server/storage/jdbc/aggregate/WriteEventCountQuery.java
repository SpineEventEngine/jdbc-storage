/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

/**
 * An abstract base for queries, which write an event count after the last
 * {@link io.spine.server.aggregate.Snapshot Snapshot} into the {@link EventCountTable}.
 */
abstract class WriteEventCountQuery<I> extends IdAwareQuery<I> implements WriteQuery {

    private final int eventCount;

    WriteEventCountQuery(Builder<? extends Builder, ? extends WriteEventCountQuery, I> builder) {
        super(builder);
        this.eventCount = builder.eventCount;
    }

    int getEventCount() {
        return eventCount;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q, I>, Q extends WriteEventCountQuery<I>, I>
            extends IdAwareQuery.Builder<I, B, Q> {

        private int eventCount;

        Builder<B, Q, I> setEventCount(int eventCount) {
            this.eventCount = eventCount;
            return getThis();
        }
    }
}
