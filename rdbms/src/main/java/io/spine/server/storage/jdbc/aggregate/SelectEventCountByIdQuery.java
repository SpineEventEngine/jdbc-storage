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

package io.spine.server.storage.jdbc.aggregate;

import com.querydsl.core.Fetchable;
import com.querydsl.core.types.dsl.PathBuilder;
import io.spine.server.storage.jdbc.query.AbstractSelectByIdQuery;

import static io.spine.server.storage.jdbc.aggregate.EventCountTable.Column.event_count;

/**
 * A query that selects event count by corresponding aggregate ID.
 *
 * @author Dmytro Grankin
 */
class SelectEventCountByIdQuery<I> extends AbstractSelectByIdQuery<I, Integer> {

    private SelectEventCountByIdQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    public Integer execute() {
        final PathBuilder<Integer> eventCount = pathOf(event_count.name(), Integer.class);
        final Fetchable<Integer> query = factory().select(eventCount)
                                                  .from(table())
                                                  .where(hasId());
        final Integer result = query.fetchOne();
        return result;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends AbstractSelectByIdQuery.Builder<I,
                                                                    Builder<I>,
                                                                    SelectEventCountByIdQuery<I>> {
        @Override
        protected SelectEventCountByIdQuery<I> doBuild() {
            return new SelectEventCountByIdQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
