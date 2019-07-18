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

package io.spine.server.storage.jdbc.message;

import com.google.protobuf.Message;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.storage.AbstractStorage;
import io.spine.server.storage.ReadRequest;
import io.spine.server.storage.jdbc.query.AbstractTable;

import java.util.Optional;

public abstract class JdbcMessageStorage<I,
        M extends Message,
        R extends ReadRequest<I>,
        T extends AbstractTable<I, M, M>>
        extends AbstractStorage<I, M, R> {

    private final T table;

    protected JdbcMessageStorage(boolean multitenant, T table) {
        super(multitenant);
        this.table = table;
    }

    @Override
    public Optional<M> read(R request) {
        I id = request.recordId();
        M result = table.read(id);
        return Optional.ofNullable(result);
    }

    @Override
    public void write(I id, M record) {
        table.write(id, record);
    }

    public void write(InboxMessage message) {

    }

    public void writeAll(Iterable<InboxMessage> messages) {

    }

    public void removeAll(Iterable<InboxMessage> messages) {

    }
}
