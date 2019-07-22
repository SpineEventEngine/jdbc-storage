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
import io.spine.server.entity.Entity;
import io.spine.server.storage.AbstractStorageTest;
import io.spine.server.storage.ReadRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class JdbcMessageStorageTest<I,
                                             M extends Message,
                                             R extends ReadRequest<I>,
                                             S extends JdbcMessageStorage<I, M, R, ?>>
        extends AbstractStorageTest<I, M, R, S> {

    /**
     * Always returns {@code null} as {@link JdbcMessageStorage} does not store any
     * {@link Entity entities}.
     *
     * <p>The method outcome is not needed by the test as the class of records stored in
     * {@link JdbcMessageStorage} is pre-defined for each descendant, unlike in other storages.
     */
    @Nullable
    @Override
    protected Class<? extends Entity<?, ?>> getTestEntityClass() {
        return null;
    }

    /**
     * Implicitly overrides the {@link io.spine.server.storage.AbstractStorageTest#HaveIndex} test.
     */
    @Nested
    @DisplayName("have index which")
    class HaveIndex {

        @Test
        @DisplayName("always throws `UnsupportedOperationException`")
        void whichAlwaysThrows() {
            assertThrows(UnsupportedOperationException.class, storage()::index);
        }
    }
}
