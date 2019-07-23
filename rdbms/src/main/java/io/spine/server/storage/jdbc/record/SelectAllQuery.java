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

package io.spine.server.storage.jdbc.record;

import com.google.protobuf.FieldMask;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.entity.EntityRecord;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.util.Iterator;

import static io.spine.server.storage.jdbc.record.QueryResults.parse;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ENTITY;

/**
 * A query selecting all {@linkplain EntityRecord entity records} from the table.
 */
final class SelectAllQuery extends AbstractQuery implements SelectQuery<Iterator<EntityRecord>> {

    private final FieldMask fieldMask;

    private SelectAllQuery(Builder builder) {
        super(builder);
        this.fieldMask = builder.fieldMask;
    }

    @Override
    public Iterator<EntityRecord> execute() {
        AbstractSQLQuery<Object, ?> query = factory().select(pathOf(ENTITY))
                                                     .from(table());
        ResultSet resultSet = query.getResults();
        Iterator<EntityRecord> iterator = parse(resultSet, fieldMask);
        return iterator;
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder extends AbstractQuery.Builder<Builder, SelectAllQuery> {

        private FieldMask fieldMask;

        Builder setFieldMask(FieldMask fieldMask) {
            this.fieldMask = fieldMask;
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
