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

package io.spine.server.storage.jdbc.type.given;

import io.spine.core.Version;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.query.Parameters;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

import java.lang.reflect.Method;

@SuppressWarnings("unused") // Entity columns are used reflectively.
public class JdbcTypeRegistryFactoryTestEnv {

    /** Prevents instantiation of this utility class. */
    private JdbcTypeRegistryFactoryTestEnv() {
    }

    public static EntityColumn booleanColumn() {
        return column("getBoolean");
    }

    public static EntityColumn versionColumn() {
        return column("getVersion");
    }

    public static EntityColumn stringColumn() {
        return column("getString");
    }

    private static EntityColumn column(String name) {
        try {
            Method method = JdbcTypeRegistryFactoryTestEnv.class.getDeclaredMethod(name);
            EntityColumn column = EntityColumn.from(method);
            return column;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    @Column
    public boolean getBoolean() {
        return false;
    }

    @Column
    public Version getVersion() {
        return Version.getDefaultInstance();
    }

    @Column
    public String getString() {
        return "";
    }

    public enum CustomType implements JdbcColumnType<String, String> {

        INSTANCE;

        @Override
        public Type getType() {
            return Type.STRING_255;
        }

        @Override
        public String convertColumnValue(String fieldValue) {
            return fieldValue;
        }

        @Override
        public void setColumnValue(Parameters.Builder storageRecord, String value,
                                   String columnIdentifier) {
            // NoOp (as for tests)
        }

        @Override
        public void setNull(Parameters.Builder storageRecord, String columnIdentifier) {
            // NoOp (as for tests)
        }
    }
}
