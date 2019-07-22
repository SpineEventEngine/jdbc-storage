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

package io.spine.server.storage.jdbc.delivery.given;

import io.spine.server.delivery.ShardIndex;

import java.security.SecureRandom;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A test utility for {@link ShardIndex} instances creation.
 */
public final class TestShardIndex {

    private static final SecureRandom random = new SecureRandom();

    private TestShardIndex() {
    }

    /**
     * Generates a new {@link ShardIndex} with random but still valid values.
     */
    public static ShardIndex generate() {
        int shardIndex = random.nextInt(100);
        int totalShards = random.nextInt(100) + 100;
        return newIndex(shardIndex, totalShards);
    }

    /**
     * Creates a new {@link ShardIndex}.
     */
    public static ShardIndex newIndex(int shardIndex, int totalShards) {
        checkArgument(shardIndex > 0,
                      "Shard index must be positive");
        checkArgument(shardIndex < totalShards,
                      "Shard index must be less than the total number of shards");
        return ShardIndex.newBuilder()
                         .setIndex(shardIndex)
                         .setOfTotal(totalShards)
                         .vBuild();
    }
}
