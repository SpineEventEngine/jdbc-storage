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

package io.spine.server.storage.jdbc.record;

import com.google.protobuf.Message;
import io.spine.protobuf.Messages;
import io.spine.server.storage.StorageGroup;

import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility class which provides strings valid for DB table names.
 */
@SuppressWarnings("UtilityClass")
public final class TableNames {

    private static final String TABLE_NAME_DELIMITER = "_";
    private static final Pattern PACKAGE_DOT = Pattern.compile("\\.");

    private TableNames() {
        // Prevent instantiation of this utility class.
    }

    /**
     * Composes the table name basing on the passed type of Proto message to store in the table.
     *
     * <p>Result consists of the Proto package of the message concatenated with the simple name
     * of the type. {@code _} symbol is used for joining the parts, and for the replacement
     * of prohibited {@code .} symbols in the name of the package.
     *
     * <p>For instance, for {@code google.protobuf.Timestamp}, the table name would be
     * {@code google_protobuf_Timestamp}.
     */
    public static String of(Class<? extends Message> cls) {
        checkNotNull(cls);
        var protoPackage =
                Messages.getDefaultInstance(cls)
                        .getDescriptorForType()
                        .getFile()
                        .getPackage();
        var preparedPackage = PACKAGE_DOT.matcher(protoPackage)
                                         .replaceAll(TABLE_NAME_DELIMITER);
        var result = preparedPackage + TABLE_NAME_DELIMITER + cls.getSimpleName();
        return result;
    }

    /**
     * Composes the name for the table storing the records of a storage which belongs
     * to a {@link StorageGroup}.
     *
     * <p>Several storages of a Bounded Context may hold records of the same type — e.g.,
     * the event journals of distinct entity types all store {@code Event}s. The group,
     * named by the framework after the state type of the served entity, is what tells
     * such storages apart. Therefore, the table name is composed of the group name
     * and the simple name of the stored record type, so that each {@code (group, record type)}
     * pair maps to its own table. {@code _} symbol is used for joining the parts,
     * and for the replacement of prohibited {@code .} symbols in the group name.
     *
     * <p>For instance, for the event journal of an entity with the state type
     * {@code spine.test.storage.StgProject}, storing {@code spine.core.Event} records,
     * the table name would be {@code spine_test_storage_StgProject_Event}.
     */
    public static String of(Class<? extends Message> recordType, StorageGroup group) {
        checkNotNull(recordType);
        checkNotNull(group);
        var preparedGroup = PACKAGE_DOT.matcher(group.getName())
                                       .replaceAll(TABLE_NAME_DELIMITER);
        var result = preparedGroup + TABLE_NAME_DELIMITER + recordType.getSimpleName();
        return result;
    }
}
