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

package io.spine.server.storage.jdbc.record.column.given;

import com.google.protobuf.Timestamp;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.test.shared.IntIdAggregate;
import io.spine.server.test.shared.LongIdAggregate;
import io.spine.server.test.shared.StringProjection;
import io.spine.server.test.shared.TimestampIdAggregate;

public final class IdColumnTestEnv {

    /** Prevents instantiation of this utility class. */
    private IdColumnTestEnv() {
    }

    public static class LongIdEntity extends AbstractEntity<Long, LongIdAggregate> {
        protected LongIdEntity(Long id) {
            super(id);
        }
    }

    public static class IntIdEntity extends AbstractEntity<Integer, IntIdAggregate> {
        protected IntIdEntity(Integer id) {
            super(id);
        }
    }

    public static class StringIdEntity extends AbstractEntity<String, StringProjection> {
        protected StringIdEntity(String id) {
            super(id);
        }
    }

    public static class MessageIdEntity extends AbstractEntity<Timestamp, TimestampIdAggregate> {
        protected MessageIdEntity(Timestamp id) {
            super(id);
        }
    }
}
