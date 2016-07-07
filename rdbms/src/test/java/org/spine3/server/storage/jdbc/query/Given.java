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

import com.google.protobuf.Any;
import com.google.protobuf.Message;

/* package */ class Given {

    /* package */ static final Any recordMock = Any.getDefaultInstance();

    /* package */ static class CreateTableMock extends CreateTableQuery<String>{

        protected CreateTableMock(Builder builder) {
            super(builder);
        }

        public static  Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends CreateTableQuery.Builder<Builder, CreateTableMock, String> {

            @Override
            public CreateTableMock build() {
                return new CreateTableMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    /* package */ static class SelectByIdQueryMock extends SelectByIdQuery<String, Message>{

        protected SelectByIdQueryMock(Builder builder) {
            super(builder);
        }

        public static  Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends SelectByIdQuery.Builder<Builder, SelectByIdQueryMock, String, Message> {

            @Override
            public SelectByIdQueryMock build() {
                return new SelectByIdQueryMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    /* package */ static class WriteRecordQueryMock extends WriteRecordQuery<String, Message>{

        protected WriteRecordQueryMock(Builder builder) {
            super(builder);
        }

        public static  Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends WriteRecordQuery.Builder<Builder, WriteRecordQueryMock, String, Message> {

            @Override
            public WriteRecordQueryMock build() {
                return new WriteRecordQueryMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    /* package */ static class WriteQueryMock extends WriteQuery{

        protected WriteQueryMock(Builder builder) {
            super(builder);
        }

        public static  Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends WriteQuery.Builder<Builder, WriteQueryMock> {

            @Override
            public WriteQueryMock build() {
                return new WriteQueryMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    private Given() {
    }
}
