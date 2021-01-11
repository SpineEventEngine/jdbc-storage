/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import io.spine.server.delivery.CatchUp;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.util.Iterator;

import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

/**
 * Selects the statuses of all catch-up processes.
 */
public class SelectAllCatchUpsQuery extends AbstractQuery
        implements SelectQuery<Iterator<CatchUp>> {

    private SelectAllCatchUpsQuery(Builder builder) {
        super(builder);
    }

    /**
     * Creates a new {@code Builder} for this query.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public Iterator<CatchUp> execute() {
        ResultSet resultSet = factory().select(pathOf(bytesColumn()))
                                       .from(table())
                                       .getResults();
        DbIterator<CatchUp> iterator =
                DbIterator.over(resultSet,
                                messageReader(bytesColumn().name(),
                                              CatchUp.getDescriptor()));
        return iterator;
    }

    /**
     * Builder of {@code SelectAllCatchUpsQuery} instance.
     */
    static class Builder extends AbstractQuery.Builder<Builder, SelectAllCatchUpsQuery> {

        /**
         * Prevents this builder from instantiating from outside of this class.
         */
        private Builder() {
            super();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectAllCatchUpsQuery doBuild() {
            return new SelectAllCatchUpsQuery(this);
        }
    }
}
