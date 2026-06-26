/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
 * SQL type names specific to MySQL, which differ from the
 * {@linkplain TypeMappingBuilder default mapping}.
 */
final class MySqlTypeNames {

    /**
     * The character set and binary collation appended to every character-based column type.
     *
     * <p>By default, MySQL uses a case- and accent-insensitive collation for non-binary
     * string types, so {@code 'name'} and {@code 'Name'} compare as equal. Entity
     * identifiers and {@code String} columns must be matched exactly; otherwise distinct
     * identifiers collide and commands for one entity get routed to another. A binary
     * collation restores exact, case-sensitive matching.
     *
     * <p>{@code utf8mb4} (rather than the deprecated {@code utf8}/{@code utf8mb3}) is used to
     * keep the full Unicode range available. The collation does not change the stored byte
     * width, so the {@code VARCHAR(512)} primary key stays within InnoDB index limits.
     */
    private static final String BINARY = "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";

    /** {@code VARCHAR(255)} with a {@linkplain #BINARY binary collation}. */
    static final String VARCHAR_255 = "VARCHAR(255) " + BINARY;

    /** {@code VARCHAR(512)} with a {@linkplain #BINARY binary collation}. */
    static final String VARCHAR_512 = "VARCHAR(512) " + BINARY;

    /** {@code TEXT} with a {@linkplain #BINARY binary collation}. */
    static final String TEXT = "TEXT " + BINARY;

    private MySqlTypeNames() {
    }
}
