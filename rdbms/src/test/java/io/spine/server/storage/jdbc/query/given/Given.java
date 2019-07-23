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

package io.spine.server.storage.jdbc.query.given;

import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;

public class Given {

    /** Prevents instantiation of this utility class. */
    private Given() {
    }

    /**
     * Returns the new builder for the test {@link SelectMessageByIdQuery} implementation.
     */
    public static ASelectMessageByIdQuery.Builder selectMessageBuilder() {
        return new ASelectMessageByIdQuery.Builder();
    }

    /**
     * Returns the new builder for the test {@link AbstractQuery} implementation.
     */
    public static AStorageQuery.Builder storageQueryBuilder() {
        return new AStorageQuery.Builder();
    }

    public static class ASelectMessageByIdQuery extends SelectMessageByIdQuery<String, Message> {

        private final AbstractSQLQuery<?, ?> query;

        private ASelectMessageByIdQuery(Builder builder) {
            super(builder);
            this.query = builder.query;
        }

        @Override
        protected AbstractSQLQuery<?, ?> query() {
            return query;
        }

        public static class Builder extends SelectMessageByIdQuery.Builder<Builder,
                                                                           ASelectMessageByIdQuery,
                                                                           String,
                                                                           Message> {

            private AbstractSQLQuery<?, ?> query;

            public Builder setQuery(AbstractSQLQuery<?, ?> query) {
                this.query = query;
                return this;
            }

            @Override
            protected ASelectMessageByIdQuery doBuild() {
                return new ASelectMessageByIdQuery(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    public static class AStorageQuery extends AbstractQuery {

        private AStorageQuery(Builder builder) {
            super(builder);
        }

        public static class Builder extends AbstractQuery.Builder<Builder, AStorageQuery> {

            @Override
            protected AStorageQuery doBuild() {
                return new AStorageQuery(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }
}
