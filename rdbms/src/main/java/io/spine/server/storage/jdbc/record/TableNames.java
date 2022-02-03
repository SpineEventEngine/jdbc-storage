/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record;

import com.google.common.hash.Hashing;
import io.spine.server.entity.Entity;

import java.sql.DatabaseMetaData;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A utility class which provides strings valid for DB table names.
 */
@SuppressWarnings("UtilityClass")
public final class TableNames {

    private TableNames() {
        // Prevent instantiation of this utility class.
    }

    /**
     * Obtains the table name basing on the specified {@link Entity} class.
     *
     * <p>The name consist of the {@link Class#getSimpleName() simple name} and short representation
     * of the {@linkplain Class#getPackage() package}, which are separated by an underscore.
     *
     * <p>A package name should be compacted, because a
     * {@linkplain DatabaseMetaData#getMaxTableNameLength() length} of the table has restrictions.
     *
     * @param cls
     *         a class of an {@linkplain Entity}
     * @return a table name from the class
     */
    //TODO:2021-12-21:alex.tymchenko: kill?
    static String newTableName(Class<? extends Entity<?, ?>> cls) {
        checkNotNull(cls);
        return compose(cls);
    }

    /**
     * Composes the table name basing on the passed class.
     *
     * //TODO:2022-01-24:alex.tymchenko: describe the details and deal with the `hashCode()`.
     */
    public static String of(Class<?> cls) {
        checkNotNull(cls);
        return compose(cls);
    }

    //TODO:2022-01-31:alex.tymchenko: document.
    private static String compose(Class<?> cls) {
        var name = cls.getPackage().getName();
        var shortPackageId = consistentHashCode(name);
        var validPackageId = Math.abs(shortPackageId);
        var packageIdAsString = String.valueOf(validPackageId);
        var result = cls.getSimpleName() + '_' + packageIdAsString;
        return result;
    }

    @SuppressWarnings("UnstableApiUsage")
    private static int consistentHashCode(String name) {
        return Hashing.murmur3_128()
                      .hashString(name, UTF_8)
                      .asInt();
    }
}
