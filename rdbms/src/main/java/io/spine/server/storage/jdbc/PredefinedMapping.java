/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.spine.type.TypeName;

import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.TypeMappingBuilder.basicBuilder;

/**
 * Predefined {@linkplain TypeMapping type mappings} for different databases.
 */
@Immutable
public enum PredefinedMapping implements TypeMapping {

    MYSQL_5_7("MySQL", 5, 7, basicBuilder()),
    POSTGRESQL_10_1("PostgreSQL", 10, 1, basicBuilder().add(BYTE_ARRAY, "BYTEA")),
    H2_2_1("H2", 2, 1, basicBuilder());

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final TypeMapping typeMapping;
    private final String databaseProductName;
    private final int majorVersion;
    private final int minorVersion;

    PredefinedMapping(String databaseProductName, int majorVersion,
                      int minorVersion, TypeMappingBuilder typeMappingBuilder) {
        this.databaseProductName = databaseProductName;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.typeMapping = typeMappingBuilder.build();
    }

    @Override
    public TypeName typeNameFor(Type type) {
        return typeMapping.typeNameFor(type);
    }

    /**
     * Selects the type mapping for the specified data source.
     *
     * <p>The {@linkplain DataSourceMetaData#productName() database product name} and the version
     * are taken into account during the selection.
     *
     * @param dataSource
     *         the data source to test suitability
     * @return the type mapping for the used database or {@linkplain PredefinedMapping#MYSQL_5_7
     *         mapping for MySQL 5.7} if there is no standard mapping for the database
     */
    static TypeMapping select(DataSourceWrapper dataSource) {
        DataSourceMetaData metaData = dataSource.metaData();
        for (PredefinedMapping mapping : values()) {
            boolean nameMatch = metaData.productName()
                                        .equals(mapping.databaseProductName);
            boolean versionMatch =
                    metaData.majorVersion() == mapping.majorVersion
                    && metaData.minorVersion() == mapping.minorVersion;
            if (nameMatch && versionMatch) {
                return mapping;
            }
        }
        return MYSQL_5_7;
    }

    @VisibleForTesting
    String getDatabaseProductName() {
        return databaseProductName;
    }

    @VisibleForTesting
    int getMajorVersion() {
        return majorVersion;
    }

    @VisibleForTesting
    int getMinorVersion() {
        return minorVersion;
    }
}
