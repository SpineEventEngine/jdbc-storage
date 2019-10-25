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

package io.spine.server.storage.jdbc.query.given;

import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnName;
import io.spine.server.entity.storage.ColumnStorageRule;
import io.spine.server.entity.storage.Columns;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.server.storage.jdbc.type.DefaultJdbcStorageRules;

public final class QueryPredicatesTestEnv {

    /** Prevents instantiation of this test env class. */
    private QueryPredicatesTestEnv() {
    }

    public static Object nonComparableValue() {
        return new Object();
    }

    public static Column stringColumn() {
        Column column = Columns.of(TestCounterEntity.class)
                               .get(ColumnName.of("id_string"));
        return column;
    }

    public static final class MapToNonComparable extends DefaultJdbcStorageRules {

        @Override
        public ColumnStorageRule<?, ?> of(Class<?> type) {
            return o -> nonComparableValue();
        }
    }
}
