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
import io.spine.server.storage.AbstractStorage;
import io.spine.server.storage.ReadRequest;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Exceptions.unsupported;

/**
 * A storage which stores plain messages in a single SQL {@linkplain MessageTable table}.
 *
 * @param <I>
 *         the record ID type
 * @param <M>
 *         the stored message type
 * @param <R>
 *         the read request type
 * @param <T>
 *         the type of used {@link MessageTable}
 */
public abstract class JdbcMessageStorage<I,
                                         M extends Message,
                                         R extends ReadRequest<I>,
                                         T extends MessageTable<I, M>>
        extends AbstractStorage<I, M, R> {

    private final T table;

    protected JdbcMessageStorage(boolean multitenant, T table) {
        super(multitenant);
        this.table = table;
        this.table.create();
    }

    @Override
    public Optional<M> read(R request) {
        checkNotNull(request);
        checkNotClosed();

        I id = request.recordId();
        M result = table.read(id);
        return Optional.ofNullable(result);
    }

    @Override
    public void write(I id, M record) {
        checkNotNull(id);
        checkNotNull(record);
        checkNotClosed();

        table.write(id, record);
    }

    /**
     * Writes a single message to the storage.
     *
     * <p>If the record with same ID already exists, it's overwritten.
     */
    public void write(M message) {
        checkNotNull(message);
        checkNotClosed();

        table.write(message);
    }

    /**
     * Writes the given messages to the storage.
     *
     * <p>If some of the given message IDs already exists, the respective records are overwritten.
     */
    public void writeAll(Iterable<M> messages) {
        checkNotNull(messages);
        checkNotClosed();

        table.writeAll(messages);
    }

    /**
     * Removes the given messages from the storage.
     *
     * <p>The messages which are not found are ignored.
     */
    public void removeAll(Iterable<M> messages) {
        checkNotNull(messages);
        checkNotClosed();

        table.removeAll(messages);
    }

    protected T table() {
        return table;
    }

    /**
     * Always throws an {@link UnsupportedOperationException}.
     */
    @Override
    public Iterator<I> index() {
        throw unsupported(
                "`JdbcMessageStorage` does not provide `index` capabilities " +
                "due to the enormous number of records stored.");
    }
}
