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

import io.spine.server.storage.jdbc.IdColumn;

import static io.spine.server.storage.jdbc.EventCountTable.Column.event_count;

/**
 * An abstract base for {@link io.spine.server.storage.jdbc.EventCountTable EventCountTable} queries.
 *
 * @author Dmytro Grankin
 */
abstract class WriteEventCountQuery<I> extends AbstractStoreQuery {

    private final I id;
    private final IdColumn<I> idColumn;
    private final int eventCount;

    WriteEventCountQuery(Builder<? extends Builder, ? extends WriteEventCountQuery, I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
        this.eventCount = builder.eventCount;
    }

    @Override
    Parameters getParameters() {
        final Parameter eventCountParameter = Parameter.of(eventCount, event_count.type());
        return Parameters.newBuilder()
                         .addParameter(event_count.name(), eventCountParameter)
                         .build();
    }

    I getId() {
        return id;
    }

    IdColumn<I> getIdColumn() {
        return idColumn;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q, I>, Q extends WriteEventCountQuery, I>
            extends AbstractStoreQuery.Builder<B, Q> {

        private IdColumn<I> idColumn;
        private I id;
        private int eventCount;

        B setId(I id) {
            this.id = id;
            return getThis();
        }

        B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        Builder<B, Q, I> setEventCount(int eventCount) {
            this.eventCount = eventCount;
            return getThis();
        }
    }
}
