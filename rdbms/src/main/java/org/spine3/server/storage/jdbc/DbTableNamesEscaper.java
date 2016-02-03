/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc;

import java.util.regex.Pattern;

/**
 * A utility class for escaping strings to be valid for DB table names.
 *
 * @author Alexander Litus
 */
@SuppressWarnings("UtilityClass")
/*package*/ class DbTableNamesEscaper {

    private static final Pattern PATTERN_DOT = Pattern.compile("\\.");
    private static final Pattern PATTERN_DOLLAR = Pattern.compile("\\$");
    private static final String UNDERSCORE = "_";

    private DbTableNamesEscaper() {}

    /**
     * Retrieves a name of a class and escapes it so that it is valid to use as a DB table name. For example:
     *
     * <p>{@code com.example.Order -> com_example_order}
     *
     * @param clazz a class which name to use
     * @return a valid DB table name
     */
    /*package*/ static String toTableName(Class clazz) {
        final String className = clazz.getName();
        final String tableNameTmp = PATTERN_DOT.matcher(className).replaceAll(UNDERSCORE);
        final String result = PATTERN_DOLLAR.matcher(tableNameTmp).replaceAll(UNDERSCORE).toLowerCase();
        return result;
    }
}
