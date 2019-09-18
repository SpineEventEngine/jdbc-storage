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
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.query.Parameters;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

import java.lang.reflect.Method;

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

    @Column
    public String getString() {
        return "";
    }

    /**
     * Returns a non-comparable object on attempt to convert an entity column value.
     *
     * <p>An entity column with such type should not be accepted during the column filter creation.
     */
    private static class NonComparableType implements JdbcColumnType<String, Object> {

        @Override
        public Object convertColumnValue(String fieldValue) {
            return new Object();
        }

        @Override
        public void setColumnValue(Parameters.Builder storageRecord, Object value,
                                   String columnIdentifier) {
            // NO-OP. This method is not interesting for test.
        }

        @Override
        public void setNull(Parameters.Builder storageRecord, String columnIdentifier) {
            // NO-OP. This method is not interesting for test.
        }

        @SuppressWarnings("ReturnOfNull") // OK for test.
        @Override
        public Type getType() {
            return null;
        }
    }
}
