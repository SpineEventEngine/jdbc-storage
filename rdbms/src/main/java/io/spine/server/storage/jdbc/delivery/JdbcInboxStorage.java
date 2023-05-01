/*
 * Copyright 2022, TeamDev. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxReadRequest;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.message.JdbcMessageStorage;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Preconditions2.checkPositive;

/**
 * A JDBC-based implementation of the {@link InboxStorage}.
 *
 * <p>All inbox messages reside in a separate SQL {@linkplain InboxTable table}.
 */
public class JdbcInboxStorage
        extends JdbcMessageStorage<InboxMessageId, InboxMessage, InboxReadRequest, InboxTable>
        implements InboxStorage {

    private final DataSourceWrapper dataSource;

    private JdbcInboxStorage(Builder builder) {
        super(builder.isMultitenant(), new InboxTable(builder.dataSource(),
                                                      builder.typeMapping()));
        this.dataSource = builder.dataSource();
    }

    /**
     * Reads the contents of the storage by the given shard index and returns the first page
     * of the results.
     *
     * <p>Older items go first.
     *
     * @param index
     *         the shard index to return the results for
     * @param pageSize
     *         the maximum number of the elements per page
     * @return the first page of the results
     */
    @Override
    public Page<InboxMessage> readAll(ShardIndex index, int pageSize) {
        checkNotNull(index);
        checkPositive(pageSize);
        Page<InboxMessage> page = new InboxPage(sinceWhen -> readAll(index, sinceWhen, pageSize));
        return page;
    }

    private ImmutableList<InboxMessage>
    readAll(ShardIndex index, @Nullable Timestamp sinceWhen, int pageSize) {
        ImmutableList<InboxMessage> result = table().readAll(index, sinceWhen, pageSize);
        return result;
    }

    @Override
    public Optional<InboxMessage> newestMessageToDeliver(ShardIndex index) {
        checkNotNull(index);
        checkNotClosed();
        Optional<InboxMessage> result = table().readOldestToDeliver(index);
        return result;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Closes the underlying data source.
     */
    @Override
    public void close() {
        super.close();
        dataSource.close();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends StorageBuilder<Builder, JdbcInboxStorage> {

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected JdbcInboxStorage doBuild() {
            return new JdbcInboxStorage(this);
        }
    }
}
