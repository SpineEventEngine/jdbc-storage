/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.examples.customquery.query;

import org.spine3.server.storage.jdbc.query.CreateTableQuery;


public class MyCreateMainTableQuery<I> extends CreateTableQuery<I> {
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s (" +
                        " id INT, " +
                        " aggregate BLOB, " +
                        " seconds BIGINT, " +
                        " nanos INT, " +
                        " customcolumn BLOB" +
                    ");";

    protected MyCreateMainTableQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    public Void execute() {
        getLogger().info("Custom query was execute. " +
                         "SQL: " + getQuery());
        return super.execute();
    }

    public static <I>Builder <I>newBuilder() {
        final Builder <I>builder = new Builder<>();
        builder.setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends CreateTableQuery.Builder<Builder<I>, MyCreateMainTableQuery, I> {

        @Override
        public MyCreateMainTableQuery build() {
            return new MyCreateMainTableQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
