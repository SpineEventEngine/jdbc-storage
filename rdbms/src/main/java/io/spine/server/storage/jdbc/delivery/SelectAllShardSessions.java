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

package io.spine.server.storage.jdbc.delivery;

import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.util.Iterator;

import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

/**
 * Selects all {@link ShardSessionRecord}s.
 */
public final class SelectAllShardSessions extends AbstractQuery
        implements SelectQuery<Iterator<ShardSessionRecord>> {

    private SelectAllShardSessions(Builder builder) {
        super(builder);
    }

    /**
     * Creates a new {@code Builder} for this query.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public Iterator<ShardSessionRecord> execute() {
        ResultSet resultSet = query().getResults();
        DbIterator<ShardSessionRecord> iterator =
                DbIterator.over(resultSet,
                                messageReader(bytesColumn().name(),
                                              ShardSessionRecord.getDescriptor()));
        return iterator;
    }

    private AbstractSQLQuery<Object, ?> query() {
        return factory().select(pathOf(bytesColumn()))
                        .from(table());
    }

    /**
     * Builder of {@code SelectAllShardSessions} query.
     */
    public static class Builder
            extends AbstractQuery.Builder<Builder, SelectAllShardSessions> {

        private Builder() {
            super();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectAllShardSessions doBuild() {
            return new SelectAllShardSessions(this);
        }
    }
}
