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
package io.spine.server.storage.jdbc.type;

import io.spine.server.storage.jdbc.Sql;

/**
 * @author Alexander Aleksandrov
 */
final class JdbcColumnTypes {

    private JdbcColumnTypes() {
        // Prevent instantiation of a utility class
    }

    /**
     * @return new instance of {@link BooleanColumnType}
     */
    static SimpleJdbcColumnType<Boolean> booleanType() {
        return new BooleanColumnType();
    }

    private static class BooleanColumnType extends SimpleJdbcColumnType<Boolean>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BOOLEAN;
        }
    }

    /**
     * @return new instance of {@link IntegerColumnType}
     */
    static SimpleJdbcColumnType<Boolean> integerType() {
        return new IntegerColumnType();
    }

    private static class IntegerColumnType extends SimpleJdbcColumnType<Boolean>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.INT;
        }
    }

    /**
     * @return new instance of {@link BigIntegerColumnType}
     */
    static SimpleJdbcColumnType<Boolean> bigIntegerType() {
        return new BigIntegerColumnType();
    }

    private static class BigIntegerColumnType extends SimpleJdbcColumnType<Boolean>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BIGINT;
        }
    }

    /**
     * @return new instance of {@link EntityColumnType}
     */
    static SimpleJdbcColumnType<Boolean> entityType() {
        return new EntityColumnType();
    }

    private static class EntityColumnType extends SimpleJdbcColumnType<Boolean>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BLOB;
        }
    }

    /**
     * @return new instance of {@link ProjectionColumnType}
     */
    static SimpleJdbcColumnType<Boolean> projectionType() {
        return new ProjectionColumnType();
    }

    private static class ProjectionColumnType extends SimpleJdbcColumnType<Boolean>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_255;
        }
    }
}
