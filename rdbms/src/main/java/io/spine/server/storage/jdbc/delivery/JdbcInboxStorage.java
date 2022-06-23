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

import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxReadRequest;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.message.JdbcMessageStorage;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

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
     * Reads the contents of the {@code Inbox} filtering by the provided shard index.
     *
     * @implNote There is no effective way to paginate relational tables via JDBC —
     *         especially if the actual content of the table is constantly changing, like in
     *         the table storing {@code InboxMessage}s.
     *         This method reads all the content for the shard into memory and emulates the
     *         actual pagination.
     *         See the <a href="https://github.com/SpineEventEngine/jdbc-storage/issues/136">
     *         respective issue</a>.
     */
    @Override
    public Page<InboxMessage> readAll(ShardIndex index, int pageSize) {
        checkNotNull(index);
        checkNotClosed();
        Iterator<InboxMessage> iterator = table().readAll(index);
        return new InboxPage(iterator, pageSize);
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
