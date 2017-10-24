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

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.core.Version;
import io.spine.json.Json;
import io.spine.server.storage.jdbc.Sql;

/**
 * A factory for basic {@link JdbcColumnType} implementations.
 *
 * @author Alexander Aleksandrov
 */
final class JdbcColumnTypes {

    private JdbcColumnTypes() {
        // Prevent instantiation of a utility class
    }

    /**
     * @return new instance of {@link JdbcColumnType} for {@code boolean} columns
     */
    static JdbcColumnType<Boolean, ?> booleanType() {
        return new BooleanColumnType();
    }

    /**
     * @return new instance of {@link JdbcColumnType} for {@code String} columns
     */
    static JdbcColumnType<String, ?> stringType() {
        return new StringColumnType();
    }

    /**
     * @return new instance of {@link JdbcColumnType} for {@code int} columns
     */
    static JdbcColumnType<Integer, ?> integerType() {
        return new IntegerColumnType();
    }

    /**
     * @return new instance of {@link JdbcColumnType} for {@code long} columns
     */
    static JdbcColumnType<Long, ?> longType() {
        return new LongColumnType();
    }

    /**
     * @return new instance of {@link JdbcColumnType} for {@link Version} columns
     */
    static JdbcColumnType<Version, ?> versionType() {
        return new VersionColumnType();
    }

    /**
     * @return new instance of {@link JdbcColumnType} for {@link Timestamp} columns
     */
    static JdbcColumnType<Timestamp, ?> timestampType() {
        return new TimestampColumnType();
    }

    /**
     * @return new instance of {@link JdbcColumnType} for
     * {@link AbstractMessage Message} columns
     * @see io.spine.server.entity.storage.ColumnTypeRegistry#get
     */
    static JdbcColumnType<AbstractMessage, ?> messageType() {
        return new MessageColumnType();
    }

    /**
     * A {@link SimpleJdbcColumnType} for {@code boolean} Entity Columns.
     */
    private static class BooleanColumnType extends SimpleJdbcColumnType<Boolean>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BOOLEAN;
        }
    }

    /**
     * A {@link SimpleJdbcColumnType} for {@code String} Entity Columns.
     */
    private static class StringColumnType extends SimpleJdbcColumnType<String>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_255;
        }
    }

    /**
     * A {@link SimpleJdbcColumnType} for {@code int} Entity Columns.
     */
    private static class IntegerColumnType extends SimpleJdbcColumnType<Integer>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.INT;
        }
    }

    /**
     * A {@link SimpleJdbcColumnType} for {@code long} Entity Columns.
     */
    private static class LongColumnType extends SimpleJdbcColumnType<Long>{

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BIGINT;
        }
    }

    /**
     * A {@link JdbcColumnType} for {@link Version} Entity Columns storing versions as
     * {@code int} values.
     */
    private static class VersionColumnType extends AbstractJdbcColumnType<Version, Integer>{

        @Override
        public Integer convertColumnValue(Version fieldValue) {
            final Integer version = fieldValue.getNumber();
            return version;
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.INT;
        }
    }

    /**
     * A {@link JdbcColumnType} for {@link Timestamp} Entity Columns storing timestamps as
     * {@code long} count of millis.
     *
     * @see Timestamps#toMillis(Timestamp)
     */
    private static class TimestampColumnType
            extends AbstractJdbcColumnType<Timestamp, Long>{

        @Override
        public Long convertColumnValue(Timestamp fieldValue) {
            final long millis = Timestamps.toMillis(fieldValue);
            return millis;
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BIGINT;
        }
    }

    /**
     * A {@link JdbcColumnType} for {@link com.google.protobuf.Message Message} Entity Columns
     * storing messages as {@code String} values in the {@linkplain Json#toCompactJson JSON format}.
     */
    private static class MessageColumnType extends AbstractJdbcColumnType<AbstractMessage, String>{

        @Override
        public String convertColumnValue(AbstractMessage fieldValue) {
            final String message = Json.toCompactJson(fieldValue);
            return message;
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_999;
        }
    }
}
