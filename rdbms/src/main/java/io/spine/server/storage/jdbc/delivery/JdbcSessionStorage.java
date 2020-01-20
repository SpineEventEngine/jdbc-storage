/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.JdbcMessageStorage;

import java.util.Iterator;

import static io.spine.util.Exceptions.unsupported;

/**
 * A JDBC-based storage for work session records.
 *
 * <p>This storage is always multitenant.
 *
 * <p>Stores all the records in a single table regardless of target entity type, shard, etc.
 */
public class JdbcSessionStorage
        extends JdbcMessageStorage<ShardIndex,
                                   ShardSessionRecord,
                                   ShardSessionReadRequest,
                                   ShardedWorkRegistryTable> {

    private JdbcSessionStorage(Builder builder) {
        this(builder.dataSource(), builder.typeMapping());
    }

    protected JdbcSessionStorage(DataSourceWrapper dataSource, TypeMapping typeMapping) {
        super(true, new ShardedWorkRegistryTable(dataSource, typeMapping));
    }

    /**
     * Obtains all the session records present in the storage.
     */
    Iterator<ShardSessionRecord> readAll() {
        return table().readAll();
    }

    /**
     * Creates a new instance of {@code Builder} for {@code JdbcSessionStorage} instances.
     *
     * @return new instance of {@code Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the {@code JdbcSessionStorage} instances.
     */
    public static final class Builder extends StorageBuilder<Builder, JdbcSessionStorage> {

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
            super();
        }

        @Override
        public Builder setMultitenant(boolean multitenant) {
            throw unsupported("`JdbcShardedWorkRegistry` is an application-wide instance " +
                                      "and therefore is always multitenant.");
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected JdbcSessionStorage doBuild() {
            return new JdbcSessionStorage(this);
        }
    }
}
