/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.delivery.given;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxMessageStatus;
import io.spine.server.delivery.ShardIndex;
import io.spine.test.delivery.Calc;
import io.spine.type.TypeUrl;

import static io.spine.base.Time.currentTime;
import static io.spine.server.delivery.given.TestInboxMessages.toDeliver;

/**
 * A test utility for {@link InboxMessage} instances creation.
 */
public final class TestInboxMessage {

    /**
     * Prevents this utility class from an instantiation.
     */
    private TestInboxMessage() {
    }

    /**
     * Generates a new {@link InboxMessage} with a random shard index.
     */
    public static InboxMessage generate() {
        ShardIndex shardIndex = TestShardIndex.newIndex();
        return generate(shardIndex, currentTime());
    }

    /**
     * Generates {@code totalMessages} in a selected shard.
     *
     * <p>Each message is generated as received {@code now} and in
     * {@link InboxMessageStatus#TO_DELIVER TO_DELIVER} status.
     */
    public static ImmutableList<InboxMessage> generateMultiple(int totalMessages,
                                                               ShardIndex index) {
        ImmutableList.Builder<InboxMessage> builder = ImmutableList.builder();
        for (int msgCounter = 0; msgCounter < totalMessages; msgCounter++) {

            InboxMessage msg = generate(index, currentTime());
            builder.add(msg);
        }
        return builder.build();
    }

    /**
     * Generates an {@link InboxMessage} with the specified values.
     *
     * <p>The message values are set as if it was received {@code now} and its status is
     * {@link InboxMessageStatus#TO_DELIVER TO_DELIVER}.
     */
    private static InboxMessage generate(ShardIndex index, Timestamp whenReceived) {
        InboxMessage message = toDeliver("target-entity-id", TypeUrl.of(Calc.class), whenReceived);
        InboxMessage.Builder asBuilder = message.toBuilder();
        InboxMessageId updatedId = asBuilder
                .getId()
                .toBuilder()
                .setIndex(index)
                .vBuild();
        asBuilder.setId(updatedId);
        return asBuilder.vBuild();
    }
}
