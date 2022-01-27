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

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.delivery.InboxStorageTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static io.spine.server.storage.jdbc.delivery.given.TestShardIndex.newIndex;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@DisplayName("`JdbcInboxStorage` should")
class JdbcInboxStorageTest extends InboxStorageTest {

    @BeforeEach
    @Override
    protected void setUpAbstractStorageTest() {
        ServerEnvironment.when(Tests.class)
                         .useStorageFactory((env) -> newFactory());
        super.setUpAbstractStorageTest();
    }

    @AfterAll
    static void tearDownClass() {
        ServerEnvironment.instance().reset();
    }

//    @Override
//    protected InboxStorage storage() {
//        return storage;
//    }
//
//    @Test
//    @DisplayName(NOT_ACCEPT_NULLS)
//    void passNullToleranceCheck() {
//        new NullPointerTester()
//                .setDefault(ShardIndex.class, newIndex(4, 5))
//                .setDefault(InboxMessage.class, InboxMessage.getDefaultInstance())
//                .setDefault(InboxMessageId.class, InboxMessageId.getDefaultInstance())
//                .setDefault(InboxReadRequest.class,
//                            new InboxReadRequest(InboxMessageId.getDefaultInstance()))
//                .testAllPublicInstanceMethods(storage);
//    }
//
//    @Test
//    @DisplayName("read and write a single `InboxMessage`")
//    void readAndWriteInboxMessage() {
//        var message = TestInboxMessage.generate();
//        storage.write(message);
//
//        var request = new InboxReadRequest(message.getId());
//        var result = storage.read(request);
//        assertTrue(result.isPresent());
//
//        var actualMessage = result.get();
//        assertEquals(message, actualMessage);
//    }
//
//    @Test
//    @DisplayName("read messages at a `ShardIndex`")
//    void readByShardIndex() {
//        var index = newIndex();
//        var messages = generateMultiple(20, index);
//        storage.writeAll(messages);
//
//        readAllAndCompare(storage, index, messages);
//    }
//
//    @Test
//    @DisplayName("remove selected `InboxMessage` instances")
//    void removeMessages() {
//        var index = newIndex();
//        var messages = generateMultiple(20, index);
//        storage.writeAll(messages);
//
//        readAllAndCompare(storage, index, messages);
//
//        var iterator = messages.iterator();
//        var first = iterator.next();
//        var second = iterator.next();
//
//        storage.removeAll(ImmutableList.of(first, second));
//
//        // Make a `List` from the rest of the elements. Those deleted aren't included.
//        var remainder = ImmutableList.copyOf(iterator);
//
//        readAllAndCompare(storage, index, remainder);
//
//        storage.removeAll(remainder);
//        checkEmpty(storage, index);
//    }
//
//    @Test
//    @DisplayName("do nothing if removing inexistent `InboxMessage` instances")
//    void doNothingIfRemovingInexistentMessages() {
//
//        var index = newIndex();
//        checkEmpty(storage, index);
//
//        var messages = generateMultiple(40, index);
//        storage.removeAll(messages);
//
//        checkEmpty(storage, index);
//    }
//
//    @Test
//    @DisplayName("mark messages delivered")
//    void markMessagedDelivered() {
//
//        var index = newIndex();
//        var messages = generateMultiple(10, index);
//        storage.writeAll(messages);
//
//        var nonDelivered = readAllAndCompare(storage, index, messages);
//        nonDelivered.iterator()
//                    .forEachRemaining((m) -> assertEquals(TO_DELIVER, m.getStatus()));
//
//        // Leave the first one in `TO_DELIVER` status and mark the rest as `DELIVERED`.
//        var iterator = messages.iterator();
//        var remainingNonDelivered = iterator.next();
//        var toMarkDelivered = ImmutableList.copyOf(iterator);
//
//        markDelivered(toMarkDelivered);
//        var originalMarkedDelivered =
//                toMarkDelivered.stream()
//                               .map(m -> m.toBuilder()
//                                          .setStatus(InboxMessageStatus.DELIVERED)
//                                          .vBuild())
//                               .collect(toImmutableList());
//
//        // Check that both `TO_DELIVER` message and those marked `DELIVERED` are stored as expected.
//        var readResult = storage.readAll(index, Integer.MAX_VALUE)
//                                .contents();
//        assertTrue(readResult.contains(remainingNonDelivered));
//        assertTrue(readResult.containsAll(originalMarkedDelivered));
//    }
//
//    private void markDelivered(ImmutableList<InboxMessage> messages) {
//        var nowDelivered = messages.stream()
//                                   .map(m -> m.toBuilder()
//                                                             .setStatus(DELIVERED)
//                                                             .vBuild())
//                                   .collect(toList());
//        storage.writeAll(nowDelivered);
//    }
//
//    @CanIgnoreReturnValue
//    private static ImmutableList<InboxMessage>
//    readAllAndCompare(InboxStorage storage, ShardIndex idx, ImmutableList<InboxMessage> expected) {
//        var page = storage.readAll(idx, Integer.MAX_VALUE);
//        assertEquals(expected.size(), page.size());
//
//        var contents = page.contents();
//        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(contents));
//        return contents;
//    }
//
//    private static void checkEmpty(InboxStorage storage, ShardIndex index) {
//        var emptyPage = storage.readAll(index, Integer.MAX_VALUE);
//        assertEquals(0, emptyPage.size());
//        assertTrue(emptyPage.contents()
//                            .isEmpty());
//        assertFalse(emptyPage.next()
//                             .isPresent());
//    }
}
