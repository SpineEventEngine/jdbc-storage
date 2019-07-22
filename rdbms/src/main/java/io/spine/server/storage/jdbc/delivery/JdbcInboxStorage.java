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

import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcInboxStorage
        extends JdbcMessageStorage<InboxMessageId,
                                   InboxMessage,
                                   InboxReadRequest,
                                   InboxTable>
        implements InboxStorage {

    /**
     * The maximum number of {@linkplain InboxMessage inbox messages} that are kept in memory
     * simultaneously during storage reads.
     *
     * <p>This value can be overridden using {@link Builder#setReadBatchSize(int)}.
     */
    private static final int DEFAULT_READ_BATCH_SIZE = 500;

    private final DataSourceWrapper dataSource;

    private JdbcInboxStorage(Builder builder) {
        super(builder.isMultitenant(), new InboxTable(builder.getDataSource(),
                                                      builder.getTypeMapping(),
                                                      builder.readBatchSize));
        this.dataSource = builder.getDataSource();
    }

    @Override
    public Page<InboxMessage> readAll(ShardIndex index) {
        checkNotNull(index);
        checkNotClosed();
        return table().readAll(index);
    }

    @Override
    public void close() {
        super.close();
        dataSource.close();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends StorageBuilder<Builder, JdbcInboxStorage> {

        private int readBatchSize = DEFAULT_READ_BATCH_SIZE;

        public Builder setReadBatchSize(int readBatchSize) {
            this.readBatchSize = readBatchSize;
            return getThis();
        }

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
