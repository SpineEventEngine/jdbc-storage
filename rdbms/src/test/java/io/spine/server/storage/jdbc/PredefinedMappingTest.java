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

package io.spine.server.storage.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_10_1;
import static io.spine.server.storage.jdbc.PredefinedMapping.select;
import static io.spine.test.Tests.nullRef;
import static io.spine.util.Exceptions.illegalStateWithCauseOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Dmytro Grankin
 */
@DisplayName("PredefinedMapping should")
class PredefinedMappingTest {

    private final PredefinedMapping mapping = POSTGRESQL_10_1;

    @Test
    @DisplayName("throw ISE if requested type has no mapping")
    void throwOnNoMapping() {
        Type notMappedType = nullRef();
        assertThrows(IllegalStateException.class, () -> MYSQL_5_7.typeNameFor(notMappedType));
    }

    @Test
    @DisplayName("be selected by database product name and major version")
    void selectTypeMapping() {
        DataSourceWrapper dataSource = dataSourceMock(mapping.getDatabaseProductName(),
                                                      mapping.getMajorVersion(),
                                                      mapping.getMinorVersion());
        assertEquals(mapping, select(dataSource));
    }

    @Test
    @DisplayName("not be selected if major versions are different")
    void notSelectForDifferentVersion() {
        String databaseProductName = mapping.getDatabaseProductName();
        int differentVersion = mapping.getMajorVersion() + 1;
        DataSourceWrapper dataSource = dataSourceMock(databaseProductName,
                                                      differentVersion,
                                                      differentVersion);
        assertNotEquals(mapping, select(dataSource));
    }

    private static DataSourceWrapper dataSourceMock(String databaseProductName,
                                                    int majorVersion,
                                                    int minorVersion) {
        DataSourceWrapper dataSource = mock(DataSourceWrapper.class);
        ConnectionWrapper connectionWrapper = mock(ConnectionWrapper.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
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
