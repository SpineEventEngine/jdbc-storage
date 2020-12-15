/*
 * Copyright 2020, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.query.IdColumn;

import javax.annotation.Nullable;

import static io.spine.server.storage.jdbc.Type.STRING_255;

public final class Column {

    /** Prevents instantiation of this test env class. */
    private Column() {
    }

    public static IdColumn<String> stringIdColumn() {
        return IdColumn.of(stringTableColumn());
    }

    public static TableColumn idTableColumn() {
        return GivenIdColumn.ID;
    }

    public static TableColumn stringTableColumn() {
        return GivenIdColumn.STRING;
    }

    private enum GivenIdColumn implements TableColumn {

        ID(null),
        STRING(STRING_255);

        @Nullable
        private final Type type;

        GivenIdColumn(@Nullable Type type) {
            this.type = type;
        }

        @Nullable
        @Override
        public Type type() {
            return type;
        }

        @Override
        public boolean isPrimaryKey() {
            return true;
        }

        @Override
        public boolean isNullable() {
            return false;
        }
    }
}
