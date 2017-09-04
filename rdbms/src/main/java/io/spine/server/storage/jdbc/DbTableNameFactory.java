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

package io.spine.server.storage.jdbc;

import io.spine.annotation.Internal;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityClass;

import java.util.regex.Pattern;

/**
 * A utility class which provides strings valid for DB table names.
 *
 * @author Alexander Litus
 */
@SuppressWarnings("UtilityClass")
@Internal
class DbTableNameFactory {

    private static final Pattern PATTERN_DOT = Pattern.compile("\\.");
    private static final Pattern PATTERN_DOLLAR = Pattern.compile("\\$");
    private static final String UNDERSCORE = "_";

    private DbTableNameFactory() {
        // Prevent instantiation of this utility class.
    }

    /**
     * Retrieves the type name of the state of the {@linkplain Entity}, whose {@linkplain Class}
     * instance is passed.
     *
     * @param cls a class of an {@linkplain Entity} whose state type name to use
     * @return a valid DB table name
     */
    static String newTableName(Class<? extends Entity<?, ?>> cls) {
        final String typeName = new EntityClass<Entity>(cls).getStateType()
                                                            .getTypeName();
        final String tableNameTmp = PATTERN_DOT.matcher(typeName)
                                               .replaceAll(UNDERSCORE);
        final String result = PATTERN_DOLLAR.matcher(tableNameTmp)
                                            .replaceAll(UNDERSCORE)
                                            .toLowerCase();
        return result;
    }
}
