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
import io.spine.base.Version;
import io.spine.json.Json;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    /**
     * @return new instance of {@link StringColumnType}
     */
    static SimpleJdbcColumnType<String> stringType() {
        return new StringColumnType();
    }

    /**
     * @return new instance of {@link IntegerColumnType}
     */
    static SimpleJdbcColumnType<Integer> integerType() {
        return new IntegerColumnType();
    }

    /**
     * @return new instance of {@link LongColumnType}
     */
    static SimpleJdbcColumnType<Long> longType() {
        return new LongColumnType();
    }

    /**
     * @return new instance of {@link VersionColumnType}
     */
    static JdbcColumnType<Version, Integer> versionType() {
        return new VersionColumnType();
    }

    /**
     * @return new instance of {@link TimestampColumnType}
     */
    static JdbcColumnType<Timestamp, java.sql.Timestamp> timestampType() {
        return new TimestampColumnType();
    }

    /**
     * @return new instance of {@link MessageColumnType}
     */
    static JdbcColumnType<AbstractMessage, String> messageType() {
        return new MessageColumnType();
    }

    private static class BooleanColumnType extends SimpleJdbcColumnType<Boolean>{

        @Override
        public void setColumnValue(PreparedStatement storageRecord, Boolean value,
                                   Integer columnIdentifier) {
            try {
                storageRecord.setBoolean(columnIdentifier, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }

        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BOOLEAN;
        }

    }

    private static class StringColumnType extends SimpleJdbcColumnType<String>{

        @Override
        public void setColumnValue(PreparedStatement storageRecord, String value,
                                   Integer columnIdentifier) {
            try {
                storageRecord.setString(columnIdentifier, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }

        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_255;
        }
    }

    private static class IntegerColumnType extends SimpleJdbcColumnType<Integer>{

        @Override
        public void setColumnValue(PreparedStatement storageRecord, Integer value,
                                   Integer columnIdentifier) {
            try {
                storageRecord.setInt(columnIdentifier, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }

        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.INT;
        }

    }

    private static class LongColumnType extends SimpleJdbcColumnType<Long>{

        @Override
        public void setColumnValue(PreparedStatement storageRecord, Long value,
                                   Integer columnIdentifier) {
            try {
                storageRecord.setLong(columnIdentifier, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BIGINT;
        }

    }

    private static class VersionColumnType extends AbstractJdbcColumnType<Version, Integer>{

        @Override
        public Integer convertColumnValue(Version fieldValue) {
            final Integer version = fieldValue.getNumber();
            return version;
        }

        @Override
        public void setColumnValue(PreparedStatement storageRecord, Integer value,
                                   Integer columnIdentifier) {
            try {
                storageRecord.setInt(columnIdentifier, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.INT;
        }

    }

    private static class TimestampColumnType
            extends AbstractJdbcColumnType<Timestamp, java.sql.Timestamp>{

        @Override
        public java.sql.Timestamp convertColumnValue(Timestamp fieldValue) {
            final java.sql.Timestamp timestamp =
                    new java.sql.Timestamp(Timestamps.toMillis(fieldValue));
            timestamp.setNanos(fieldValue.getNanos());
            return timestamp;
        }

        @Override
        public void setColumnValue(PreparedStatement storageRecord, java.sql.Timestamp value,
                                   Integer columnIdentifier) {
            try {
                storageRecord.setTimestamp(columnIdentifier, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BLOB;
        }

    }

    private static class MessageColumnType extends AbstractJdbcColumnType<AbstractMessage, String>{

        @Override
        public String convertColumnValue(AbstractMessage fieldValue) {
            final String message = Json.toCompactJson(fieldValue);
            return message;
        }

        @Override
        public void setColumnValue(PreparedStatement storageRecord, String value,
                                   Integer columnIdentifier) {
            try {
                storageRecord.setString(columnIdentifier, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }

        }

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.BLOB;
        }

    }
}
