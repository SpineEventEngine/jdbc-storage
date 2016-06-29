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

package org.spine3.server.storage.jdbc.query;

import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import java.sql.PreparedStatement;

/**
 * @author Andrey Lavrov
 */
public class AbstractQuery {

    private final String query;
    protected final DataSourceWrapper dataSource;

    protected AbstractQuery(Builder<? extends Builder, ? extends AbstractQuery> builder) {
        this.query = builder.query;
        this.dataSource = builder.dataSource;
    }

    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        return connection.prepareStatement(query);
    }

    public String getQuery() {
        return query;
    }

    public abstract static class Builder<B extends Builder<B, Q>, Q extends AbstractQuery> {

        private DataSourceWrapper dataSource;
        private String query;

        public abstract Q build();

        protected abstract B getThis();

        public B setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return getThis();
        }

        public B setQuery(String query) {
            this.query = query;
            return getThis();
        }
    }

}
