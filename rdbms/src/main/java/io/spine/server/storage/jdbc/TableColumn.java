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

import io.spine.server.storage.StorageField;

import javax.annotation.Nullable;

/**
 * An interface for the database table columns representation.
 *
 * <p>It's recommended to implement this interface in an {@code enum}, since it's API is sharpened
 * to be overridden with the {@code enum} default methods.
 *
 * @author Dmytro Dashenkov
 */
public interface TableColumn extends StorageField {

    /**
     * @return the name of the column
     */
    String name();

    /**
     * @return the {@link Type} of the column
     *         or {@code null} if the type is unknown at the compile time
     */
    @Nullable
    Type type();

    /**
     * @return {@code true} is this column is a primary key of the table, {@code false} otherwise
     */
    boolean isPrimaryKey();

    /**
     * @return {@code true} if this column may contain {@code NULL} values, {@code false} otherwise
     */
    boolean isNullable();
}
