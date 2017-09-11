/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.given;

import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.storage.Column;
import io.spine.server.storage.RecordStorageShould.TestCounterEntity;
import io.spine.test.storage.Project;

/**
 * @author Dmytro Grankin
 */
public class JdbcRecordStorageTestEnv {

    public static final String COLUMN_NAME_FOR_STORING = "customName";

    private JdbcRecordStorageTestEnv() {
        // Prevent instantiation of this utility class.
    }

    public static class TestCounterEntityJdbc extends TestCounterEntity<String> {
        protected TestCounterEntityJdbc(String id) {
            super(id);
        }
    }

    public static class TestEntityWithStringId extends AbstractEntity<String, Project> {
        protected TestEntityWithStringId(String id) {
            super(id);
        }

        @Column(name = COLUMN_NAME_FOR_STORING)
        public int getValue() {
            return 0;
        }
    }
}