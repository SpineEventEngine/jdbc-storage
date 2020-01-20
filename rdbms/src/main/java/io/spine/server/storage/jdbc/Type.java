/*
 * Copyright 2020, TeamDev. All rights reserved.
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
 * Data types used in the SQL tables of the project.
 *
 * <p>These types are abstract and have no relation to a particular SQL database.
 *
 * <p>The names of the types for a particular database are specified by a {@link TypeMapping}.
 */
public enum Type {

    /**
     * The type representing a byte array.
     */
    BYTE_ARRAY,

    /**
     * The type representing an {@code int} value.
     */
    INT,

    /**
     * The type representing a {@code long} value.
     */
    LONG,

    /**
     * The type representing a {@code String}, maximum length of which
     * doesn't exceed 255 characters.
     */
    STRING_255,

    /**
     * The type representing a {@code String}, maximum length of which is unknown.
     */
    STRING,

    /**
     * The type representing a {@code boolean} value.
     */
    BOOLEAN
}
