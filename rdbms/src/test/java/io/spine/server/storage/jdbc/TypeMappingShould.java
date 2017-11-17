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

import io.spine.server.storage.jdbc.TypeMapping.Builder;
import io.spine.server.storage.jdbc.TypeMapping.DatabaseProductName;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.TypeMapping.DatabaseProductName.MySQL;
import static io.spine.server.storage.jdbc.StandardMappings.mySql;
import static io.spine.test.Tests.nullRef;
import static io.spine.util.Exceptions.illegalStateWithCauseOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Dmytro Grankin
 */
public class TypeMappingShould {

    @Test
    public void allow_override_type_names_during_building() {
        final Type type = Type.BYTE_ARRAY;
        final String originalName = "original";
        final String nameReplacement = "replacement";
        final TypeMapping mapping = TypeMapping.newBuilder()
                                               .add(type, originalName)
                                               .add(type, nameReplacement)
                                               .build();
        final String resultingName = mapping.getTypeName(type);
        assertEquals(nameReplacement, resultingName);
    }

    @Test
    public void match_for_data_source_by_database_product_name_and_major_version() {
        final int majorVersion = 5;
        final DatabaseProductName databaseProductName = MySQL;
        final TypeMapping mapping = StandardMappings.baseBuilder()
                                                    .setDatabaseName(databaseProductName)
                                                    .setMajorVersion(majorVersion)
                                                    .build();
        final DataSourceWrapper dataSource = dataSourceMock(databaseProductName, majorVersion);
        assertTrue(mapping.suitableFor(dataSource));
    }

    @Test
    public void not_match_for_data_source_if_major_versions_different() {
        final int mappingVersion = 5;
        final DatabaseProductName databaseProductName = MySQL;
        final TypeMapping mapping = StandardMappings.baseBuilder()
                                                    .setDatabaseName(databaseProductName)
                                                    .setMajorVersion(mappingVersion)
                                                    .build();
        final int differentVersion = mappingVersion + 1;
        final DataSourceWrapper dataSource = dataSourceMock(databaseProductName, differentVersion);
        assertFalse(mapping.suitableFor(dataSource));
    }

    @Test
    public void not_match_if_database_product_name_null() {
        final TypeMapping mapping = StandardMappings.baseBuilder()
                                                    .build();
        assertNull(mapping.getDatabaseProductName());

        final DataSourceWrapper dataSource = dataSourceMock(MySQL, 0);
        assertFalse(mapping.suitableFor(dataSource));
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_if_not_all_types_mapped() {
        final Builder builder = TypeMapping.newBuilder();
        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void throw_ISE_if_requested_type_has_no_mapping() {
        final Type notMappedType = nullRef();
        mySql().getTypeName(notMappedType);
    }

    private static DataSourceWrapper dataSourceMock(DatabaseProductName databaseProductName,
                                                    int majorVersion) {
        final DataSourceWrapper dataSource = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionWrapper = mock(ConnectionWrapper.class);
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        doReturn(connectionWrapper).when(dataSource)
                                   .getConnection(anyBoolean());
        doReturn(connection).when(connectionWrapper)
                            .get();
        try {
            doReturn(metadata).when(connection)
                              .getMetaData();
            doReturn(databaseProductName.name()).when(metadata)
                                                .getDatabaseProductName();
            doReturn(majorVersion).when(metadata)
                                  .getDatabaseMajorVersion();
            return dataSource;
        } catch (SQLException e) {
            throw illegalStateWithCauseOf(e);
        }
    }
}
