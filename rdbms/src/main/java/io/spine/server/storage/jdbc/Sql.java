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

/**
 * Set of enums and utilities for constructing the SQL sentences.
 *
 * <p>Defines the common SQL keywords, operators and punctuation. They serve as reusable parts to
 * build SQL expressions.
 *
 * <p>All the {@code enum} values have a valid token string representation, i.e.
 * {@link Enum#toString() toString()} method returns a valid SQL token wrapped into the whitespaces.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
public final class Sql {

    private Sql() {
        // Prevent utility class instantiation.
    }

    /**
     * Set of SQL keywords representing basic data types used in the project.
     */
    public enum Type {

        /**
         * The type of a generic ID.
         *
         * <p>Use this type for an ID {@linkplain TableColumn column},
         * type of which can be determined only at the runtime.
         *
         * <p>E.g. {@link io.spine.server.aggregate.AggregateStorage AggregateStorage} has
         * a generic ID, that can be {@code int}, {@code long}, {@code Message} etc.
         * So an ID type in these cases should be determined at the runtime.
         *
         * <p>This is not designed to serve as a "dynamic" type which can be replaced in any time,
         * but only to solve the problem if identifiers with unknown types. Using this type for
         * a non-ID column may lead to a failure.
         */
        ID("generic ID type"),
        BLOB("BLOB"),
        TIMESTAMP("TIMESTAMP"),
        INT("INT"),
        BIGINT("BIGINT"),
        VARCHAR_255("VARCHAR(255)"),
        VARCHAR_512("VARCHAR(512)"),
        VARCHAR_999("VARCHAR(999)"),
        BOOLEAN("BOOLEAN");

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
     * Set of basic SQL keywords/key-phrases for CRUD operations, predicate constructing,
     * grouping and ordering, etc.
     */
    public enum Query {

        CREATE_TABLE("CREATE TABLE"),
        CREATE_IF_MISSING("CREATE TABLE IF NOT EXISTS"),
        DROP_TABLE("DROP TABLE"),
        PRIMARY_KEY("PRIMARY KEY"),
        DEFAULT,

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
        IS,
        EXISTS,
        BETWEEN,
        TRUE,
        FALSE,

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
     *
     * <ul>
     *      <li>MIN
     *      <li>MAX
     *      <li>COUNT
     *      <li>AVG
     *      <li>SUM
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
     *
     * <ul>
     *     <li>Operators: equal, not equal, comparison operators;
     *     <li>Punctuation: comma, brackets, semicolon.
     * </ul>
     */
    public enum BuildingBlock {

        COMMA(","),
        BRACKET_OPEN("("),
        BRACKET_CLOSE(")"),
        EQUAL("="),
        NOT_EQUAL("<>"),
        GREATER_THAN(">"),
        GREATER_OR_EQUAL(">="),
        LESS_THAN("<"),
        LESS_OR_EQUAL("<="),
        SEMICOLON(";");

        private final String token;

        BuildingBlock(String token) {
            this.token = token;
        }

        @Override
        public String toString() {
            return ' ' + token + ' ';
        }
    }
}
