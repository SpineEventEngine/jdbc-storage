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

package io.spine.server.storage.jdbc.record;

import com.google.protobuf.FieldMask;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.client.OrderBy;
import io.spine.server.entity.EntityRecord;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.record.QueryResults.parse;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ENTITY;

/**
 * A query selecting all {@linkplain EntityRecord entity records} from the table.
 */
final class SelectAllQuery extends AbstractQuery implements SelectQuery<Iterator<EntityRecord>> {

    private final FieldMask fieldMask;
    private final @Nullable OrderBy ordering;
    private final @Nullable Integer limit;

    private SelectAllQuery(Builder builder) {
        super(builder);
        this.fieldMask = builder.fieldMask;
        this.ordering = builder.ordering;
        this.limit = builder.limit;
    }

    @Override
    public Iterator<EntityRecord> execute() {
        AbstractSQLQuery<Object, ?> query = factory().select(pathOf(ENTITY))
                                                     .from(table());
        query = addOrderingAndLimit(query, ordering, limit);
        ResultSet resultSet = query.getResults();
        Iterator<EntityRecord> iterator = parse(resultSet, fieldMask);
        return iterator;
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder extends AbstractQuery.Builder<Builder, SelectAllQuery> {

        private FieldMask fieldMask;
        private OrderBy ordering;
        private Integer limit;


        Builder setFieldMask(FieldMask fieldMask) {
            this.fieldMask = checkNotNull(fieldMask);
            return getThis();
        }

        Builder setOrdering(OrderBy ordering) {
            this.ordering = ordering;
            return getThis();
        }

        Builder setLimit(int limit) {
            checkArgument(limit >= 0);
            this.limit = limit;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectAllQuery doBuild() {
            return new SelectAllQuery(this);
        }
    }
}
