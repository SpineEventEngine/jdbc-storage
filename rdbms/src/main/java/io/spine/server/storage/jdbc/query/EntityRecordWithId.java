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

package io.spine.server.storage.jdbc.query;

import io.spine.server.entity.EntityRecord;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A combination of the {@link EntityRecord} and its ID for iterating over the SQL query response.
 *
 * @param <I>
 *         the ID type
 */
public final class EntityRecordWithId<I> {

    private final I id;
    private final EntityRecord record;

    private EntityRecordWithId(I id, EntityRecord record) {
        this.id = id;
        this.record = record;
    }

    /**
     * Creates a new {@code EntityRecordWithId} instance.
     */
    public static <I> EntityRecordWithId<I> of(I id, EntityRecord record) {
        checkNotNull(id);
        checkNotNull(record);
        return new EntityRecordWithId<>(id, record);
    }

    public I id() {
        return id;
    }

    public EntityRecord record() {
        return record;
    }
}
