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

import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.jdbc.ColumnTypeRegistry;
import io.spine.server.storage.jdbc.PersistenceStrategy;
import io.spine.server.storage.jdbc.Type;

import java.lang.reflect.Method;

import static io.spine.server.storage.jdbc.Type.STRING;

public final class QueryPredicatesTestEnv {

    /** Prevents instantiation of this test env class. */
    private QueryPredicatesTestEnv() {
    }

    public static NonComparableType nonComparableType() {
        return new NonComparableType();
    }

    public static EntityColumn stringColumn() {
        try {
            Method method = QueryPredicatesTestEnv.class.getDeclaredMethod("getString");
            EntityColumn column = EntityColumn.from(method);
            return column;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class NonComparableRegistry implements ColumnTypeRegistry {

        @Override
        public <T> PersistenceStrategy<T> persistenceStrategyOf(Class<T> clazz) {
            return t -> new Object();
        }

        @Override
        public Type typeOf(Class<?> clazz) {
            return STRING;
        }
    }
}
