/*
 * Copyright 2018, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import io.spine.server.entity.Entity;

import java.sql.DatabaseMetaData;

/**
 * A utility class which provides strings valid for DB table names.
 *
 * @author Alexander Litus
 */
@SuppressWarnings("UtilityClass")
class DbTableNameFactory {

    private DbTableNameFactory() {
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
     * @param cls a class of an {@linkplain Entity}
     * @return a table name from the class
     */
    static String newTableName(Class<? extends Entity<?, ?>> cls) {
        int shortPackageId = cls.getPackage()
                                .hashCode();
        // The minus is an invalid sign in a table name.
        int validPackageId = Math.abs(shortPackageId);
        String packageIdAsString = String.valueOf(validPackageId);
        String result = cls.getSimpleName() + '_' + packageIdAsString;
        return result;
    }
}
