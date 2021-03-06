/*
 * Copyright 2021, TeamDev. All rights reserved.
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.testing.NullPointerTester;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxMessageStatus;
import io.spine.server.delivery.InboxReadRequest;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.delivery.InboxStorageTest;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.delivery.given.TestInboxMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.spine.server.delivery.InboxMessageStatus.DELIVERED;
import static io.spine.server.delivery.InboxMessageStatus.TO_DELIVER;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_1_4;
import static io.spine.server.storage.jdbc.delivery.given.TestInboxMessage.generateMultiple;
import static io.spine.server.storage.jdbc.delivery.given.TestShardIndex.newIndex;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@DisplayName("JdbcInboxStorage should")
class JdbcInboxStorageTest extends InboxStorageTest {

    private JdbcInboxStorage storage;

    @Override
    @BeforeEach
    protected void setUp() {
        DataSourceWrapper dataSource = whichIsStoredInMemory("jdbcInboxStorageTest");
        storage = JdbcInboxStorage
                .newBuilder()
                .setDataSource(dataSource)
                .setMultitenant(false)
                .setTypeMapping(H2_1_4)
                .build();
        super.setUp();
    }

    @Override
    protected InboxStorage storage() {
        return storage;
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .setDefault(ShardIndex.class, newIndex(4, 5))
                .setDefault(InboxMessage.class, InboxMessage.getDefaultInstance())
                .setDefault(InboxMessageId.class, InboxMessageId.getDefaultInstance())
                .setDefault(InboxReadRequest.class,
                            new InboxReadRequest(InboxMessageId.getDefaultInstance()))
                .testAllPublicInstanceMethods(storage);
    }

    @Test
    @DisplayName("read and write a single `InboxMessage`")
    void readAndWriteInboxMessage() {
        InboxMessage message = TestInboxMessage.generate();
        storage.write(message);

        InboxReadRequest request = new InboxReadRequest(message.getId());
        Optional<InboxMessage> result = storage.read(request);
        assertTrue(result.isPresent());

        InboxMessage actualMessage = result.get();
        assertEquals(message, actualMessage);
    }

    @Test
    @DisplayName("read messages at a `ShardIndex`")
    void readByShardIndex() {
        ShardIndex index = newIndex();
        ImmutableList<InboxMessage> messages = generateMultiple(20, index);
        storage.writeAll(messages);

        readAllAndCompare(storage, index, messages);
    }

    @Test
    @DisplayName("remove selected `InboxMessage` instances")
    void removeMessages() {
        ShardIndex index = newIndex();
        ImmutableList<InboxMessage> messages = generateMultiple(20, index);
        storage.writeAll(messages);

        readAllAndCompare(storage, index, messages);

        UnmodifiableIterator<InboxMessage> iterator = messages.iterator();
        InboxMessage first = iterator.next();
        InboxMessage second = iterator.next();

        storage.removeAll(ImmutableList.of(first, second));

        // Make a `List` from the rest of the elements. Those deleted aren't included.
        ImmutableList<InboxMessage> remainder = ImmutableList.copyOf(iterator);

        readAllAndCompare(storage, index, remainder);

        storage.removeAll(remainder);
        checkEmpty(storage, index);
    }

    @Test
    @DisplayName("do nothing if removing inexistent `InboxMessage` instances")
    void doNothingIfRemovingInexistentMessages() {

        ShardIndex index = newIndex();
        checkEmpty(storage, index);

        ImmutableList<InboxMessage> messages = generateMultiple(40, index);
        storage.removeAll(messages);

        checkEmpty(storage, index);
    }

    @Test
    @DisplayName("mark messages delivered")
    void markMessagedDelivered() {

        ShardIndex index = newIndex();
        ImmutableList<InboxMessage> messages = generateMultiple(10, index);
        storage.writeAll(messages);

        ImmutableList<InboxMessage> nonDelivered = readAllAndCompare(storage, index, messages);
        nonDelivered.iterator()
                    .forEachRemaining((m) -> assertEquals(TO_DELIVER, m.getStatus()));

        // Leave the first one in `TO_DELIVER` status and mark the rest as `DELIVERED`.
        UnmodifiableIterator<InboxMessage> iterator = messages.iterator();
        InboxMessage remainingNonDelivered = iterator.next();
        ImmutableList<InboxMessage> toMarkDelivered = ImmutableList.copyOf(iterator);

        markDelivered(toMarkDelivered);
        ImmutableList<InboxMessage> originalMarkedDelivered =
                toMarkDelivered.stream()
                               .map(m -> m.toBuilder()
                                          .setStatus(InboxMessageStatus.DELIVERED)
                                          .vBuild())
                               .collect(toImmutableList());

        // Check that both `TO_DELIVER` message and those marked `DELIVERED` are stored as expected.
        ImmutableList<InboxMessage> readResult = storage.readAll(index, Integer.MAX_VALUE)
                                                        .contents();
        assertTrue(readResult.contains(remainingNonDelivered));
        assertTrue(readResult.containsAll(originalMarkedDelivered));
    }

    private void markDelivered(ImmutableList<InboxMessage> messages) {
        List<InboxMessage> nowDelivered = messages.stream()
                                                  .map(m -> m.toBuilder()
                                                             .setStatus(DELIVERED)
                                                             .vBuild())
                                                  .collect(toList());
        storage.writeAll(nowDelivered);
    }

    @CanIgnoreReturnValue
    private static ImmutableList<InboxMessage>
    readAllAndCompare(InboxStorage storage, ShardIndex idx, ImmutableList<InboxMessage> expected) {
        Page<InboxMessage> page = storage.readAll(idx, Integer.MAX_VALUE);
        assertEquals(expected.size(), page.size());

        ImmutableList<InboxMessage> contents = page.contents();
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(contents));
        return contents;
    }

    private static void checkEmpty(InboxStorage storage, ShardIndex index) {
        Page<InboxMessage> emptyPage = storage.readAll(index, Integer.MAX_VALUE);
        assertEquals(0, emptyPage.size());
        assertTrue(emptyPage.contents()
                            .isEmpty());
        assertFalse(emptyPage.next()
                             .isPresent());
    }
}
