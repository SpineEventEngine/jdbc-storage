/*
 * Copyright 2017, TeamDev. All rights reserved.
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

import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_10_1;
import static io.spine.server.storage.jdbc.PredefinedMapping.select;
import static io.spine.test.Tests.nullRef;
import static io.spine.util.Exceptions.illegalStateWithCauseOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Dmytro Grankin
 */
public class PredefinedMappingShould {

    private final PredefinedMapping mapping = POSTGRESQL_10_1;

    @Test(expected = IllegalStateException.class)
    public void throw_ISE_if_requested_type_has_no_mapping() {
        final Type notMappedType = nullRef();
        MYSQL_5_7.typeNameFor(notMappedType);
    }

    @Test
    public void be_selected_by_database_product_name_and_major_version() {
        final DataSourceWrapper dataSource = dataSourceMock(mapping.getDatabaseProductName(),
                                                            mapping.getMajorVersion(),
                                                            mapping.getMinorVersion());
        assertEquals(mapping, select(dataSource));
    }

    @Test
    public void not_be_selected_if_major_versions_different() {
        final String databaseProductName = mapping.getDatabaseProductName();
        final int differentVersion = mapping.getMajorVersion() + 1;
        final DataSourceWrapper dataSource = dataSourceMock(databaseProductName,
                                                            differentVersion,
                                                            differentVersion);
        assertNotEquals(mapping, select(dataSource));
    }

    private static DataSourceWrapper dataSourceMock(String databaseProductName,
                                                    int majorVersion,
                                                    int minorVersion) {
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
            doReturn(databaseProductName).when(metadata)
                                         .getDatabaseProductName();
            doReturn(majorVersion).when(metadata)
                                  .getDatabaseMajorVersion();
            doReturn(minorVersion).when(metadata)
                                  .getDatabaseMinorVersion();
            return dataSource;
        } catch (SQLException e) {
            throw illegalStateWithCauseOf(e);
        }
    }
}
