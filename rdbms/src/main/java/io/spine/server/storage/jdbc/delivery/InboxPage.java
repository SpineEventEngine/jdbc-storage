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
import io.spine.server.delivery.Page;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Optional;

/**
 * A page of messages obtained from the {@link JdbcInboxStorage}.
 */
final class InboxPage implements Page<InboxMessage> {

    private final Lookup lookup;
    private final ImmutableList<InboxMessage> contents;

    private @MonotonicNonNull Timestamp whenLastRead = null;

    /**
     * Creates a new page with the specified way to read the next messages.
     */
    InboxPage(Lookup lookup) {
        this.lookup = lookup;
        this.contents = readNext();
    }

    /**
     * Creates a page next to the previous one, with the initial contents preloaded.
     *
     * @param previous
     *         page that preceded the one being created
     * @param initialContents
     *         the initial contents of this newly created page instance
     */
    private InboxPage(InboxPage previous, ImmutableList<InboxMessage> initialContents) {
        this.lookup = previous.lookup;
        this.whenLastRead = previous.whenLastRead;
        this.contents = initialContents;
    }

    @Override
    public ImmutableList<InboxMessage> contents() {
        return contents;
    }

    @Override
    public int size() {
        return contents.size();
    }

    /**
     * Loads a content for the next page and returns a new instance of the {@code InboxPage}.
     *
     * <p>In case there were no messages loaded, this page is considered to be the last one,
     * and {@code Optional.empty()} is returned.
     */
    @Override
    public Optional<Page<InboxMessage>> next() {
        ImmutableList<InboxMessage> moreContent = readNext();
        if (moreContent.isEmpty()) {
            return Optional.empty();
        }
        InboxPage nextPage = new InboxPage(this, moreContent);
        return Optional.of(nextPage);
    }

    private ImmutableList<InboxMessage> readNext() {
        ImmutableList<InboxMessage> contents = lookup.readAll(whenLastRead);
        if (!contents.isEmpty()) {
            this.whenLastRead = contents.get(contents.size() - 1)
                                        .getWhenReceived();
        }
        return contents;
    }

    /**
     * A strategy on fetching the {@link InboxMessage}s from the storage based
     * on the passed timestamp.
     */
    interface Lookup {

        /**
         * Reads the messages which were received strictly later than the specified
         * {@code sinceWhen} value.
         *
         * <p>If the passed value is {@code null}, the time filtering is not applied.
         *
         * @param sinceWhen
         *         the time since when the messages should be read; all satisfying messages
         *         must be received strictly later than this value;
         *         {@code null} if no filtering should be applied
         * @return the iterator over the results
         */
        ImmutableList<InboxMessage> readAll(@Nullable Timestamp sinceWhen);
    }
}
