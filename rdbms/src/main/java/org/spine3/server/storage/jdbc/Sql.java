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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Set of enums and utilities for constructing the SQL sentences.
 * <p>Contains most valid SQL keywords/key-phrases, operators and punctuation.
 *
 * <p>All the {@code enum} values have a valid token string representation, i.e. {@link Enum#toString() toString()}
 * method returns a valid SQL token wrapped into the whitespaces.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
public class Sql {

    private Sql() {
    }

    /**
     * Generates a sequence of SQL {@link Sql.Query#PLACEHOLDER placeholders} of given length in format:
     * <pre>
     *     (?, ?, ... ?, ?)
     *      \_____  _____/
     *            \/
     *          count
     * </pre>
     *
     * @param count count of the placeholders to be generated
     * @return a string of the placeholders of given count separated by commas and wrapped into the braces
     */
    public static String nPlaceholders(int count) {
        checkArgument(count > 0, "Count of placeholders should be > 0");
        final StringBuilder result = new StringBuilder(count * 2 + 1);
        result.append(Common.BRACKET_OPEN);
        for (int i = 0; i < count; i++) {
            result.append(Query.PLACEHOLDER);
            if (i + 1 != count) { // Unless last iteration
                result.append(Common.COMMA);
            }
        }
        result.append(Common.BRACKET_CLOSE);

        return result.toString();
    }

    /**
     * Set of SQL keywords representing basic data types used in the project.
     */
    public enum Type {

        BLOB("BLOB"),
        INT("INT"),
        BIGINT("BIGINT"),
        VARCHAR_512("VARCHAR(512)"),
        VARCHAR_999("VARCHAR(999)");

        private final String token;

        Type(String token) {
            this.token = token;
        }

        @Override
        public String toString() {
            return ' ' + token + ' ';
        }
    }

    /**
     * Set of basic SQL keywords/key-phrases for CRUD operations, predicate constructing, grouping and ordering, etc.
     */
    public enum Query {

        CREATE_TABLE("CREATE TABLE"),
        CREATE_IF_MISSING("CREATE TABLE IF NOT EXISTS"),
        DROP_TABLE("DROP TABLE"),
        PRIMARY_KEY("PRIMARY KEY"),

        INSERT_INTO("INSERT INTO"),
        SELECT,
        UPDATE,
        DELETE_FROM("DELETE FROM"),


        ALL_ATTRIBUTES("*"),
        FROM,
        DISTINCT,
        WHERE,
        SET,
        VALUES,

        AND,
        OR,
        NULL,
        LIKE,
        NOT,
        IN,
        EXISTS,
        BETWEEN,

        PLACEHOLDER("?"),

        GROUP_BY("GROUP BY"),
        ORDER_BY("ORDER BY"),
        HAVING,
        ASC,
        DESC;

        private final String token;

        Query(String token) {
            this.token = token;
        }

        Query() {
            this.token = name();
        }

        @Override
        public String toString() {
            return ' ' + token + ' ';
        }
    }

    /**
     * Set of SQL keywords representing 5 aggregating functions:
     * <ul>
     *     <li>MIN
     *     <li>MAX
     *     <li>COUNT
     *     <li>AVG
     *     <li>SUM
     * </ul>
     */
    public enum Function {

        MIN,
        MAX,
        COUNT,
        AVG,
        SUM;

        @Override
        public String toString() {
            return ' ' + name() + ' ';
        }
    }

    /**
     * Set of punctuation signs used in SQL:
     * <ul>
     *     <li>Operators: equal, not equal, comparison operators;
     *     <li>Signs: comma, brackets, semicolon.
     * </ul>
     */
    public enum Common {

        COMMA(","),
        BRACKET_OPEN("("),
        BRACKET_CLOSE(")"),
        EQUAL("="),
        NOT_EQUAL("<>"),
        GT(">"),
        GE(">="),
        LT("<"),
        LE("<="),
        SEMICOLON(";");

        private final String token;

        Common(String token) {
            this.token = token;
        }

        @Override
        public String toString() {
            return ' ' + token + ' ';
        }
    }
}
