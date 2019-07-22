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

import com.google.common.collect.ImmutableList;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.Page;

import java.util.Iterator;
import java.util.Optional;

final class InboxPage implements Page<InboxMessage> {

    private final Iterator<InboxMessage> iterator;
    private final int size;
    private final ImmutableList<InboxMessage> contents;

    InboxPage(Iterator<InboxMessage> iterator, int size) {
        this.iterator = iterator;
        this.size = size;
        ImmutableList.Builder<InboxMessage> builder = transform(iterator, size);
        contents = builder.build();
    }

    private static ImmutableList.Builder<InboxMessage> transform(Iterator<InboxMessage> it,
                                                                 int size) {
        ImmutableList.Builder<InboxMessage> builder = ImmutableList.builder();
        int contentSize = 0;
        while (contentSize < size && it.hasNext()) {
            InboxMessage message = it.next();
            builder.add(message);
            contentSize++;
        }
        return builder;
    }

    @Override
    public ImmutableList<InboxMessage> contents() {
        return contents;
    }

    @Override
    public int size() {
        return contents.size();
    }

    @Override
    public Optional<Page<InboxMessage>> next() {
        if (!iterator.hasNext()) {
            return Optional.empty();
        }
        InboxPage page = new InboxPage(iterator, size);
        return Optional.of(page);
    }
}
