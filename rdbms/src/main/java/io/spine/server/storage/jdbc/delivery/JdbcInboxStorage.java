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
import io.spine.server.storage.jdbc.message.JdbcMessageStorage;

import java.util.Iterator;

import static io.spine.util.Exceptions.unsupported;

public class JdbcInboxStorage
        extends JdbcMessageStorage<InboxMessageId,
                                   InboxMessage,
                                   InboxReadRequest,
                                   InboxMessageTable>
        implements InboxStorage {

    protected JdbcInboxStorage(boolean multitenant, InboxMessageTable table) {
        super(multitenant, table);
    }

    @Override
    public Page<InboxMessage> readAll(ShardIndex index) {
        return table().readAll(index);
    }

    /**
     * Always throws an {@link UnsupportedOperationException}.
     */
    @Override
    public Iterator<InboxMessageId> index() {
        throw unsupported(
            "`JdbcInboxStorage` does not provide `index` capabilities " +
            "due to the enormous number of records stored.");
    }
}
