/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import io.spine.server.storage.jdbc.GivenDataSource.ThrowingHikariDataSource;
import io.spine.testing.logging.MuteLogging;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsThrowingByCommand;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_1_4;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("DataSourceWrapper should")
class DataSourceWrapperTest {

    @Test
    @MuteLogging
    @DisplayName("throw `DatabaseException` if failing to close")
    void throwIfFailToClose() {
        ThrowingHikariDataSource dataSource = whichIsThrowingByCommand(newUuid());
        DataSourceWrapper wrapper = DataSourceWrapper.wrap(dataSource);
        dataSource.setThrowOnClose(true);
        assertThrows(DatabaseException.class, wrapper::close);
    }

    @Test
    @DisplayName("provide database meta data")
    void returnMetaData() {
        DataSourceWrapper dataSourceWrapper = whichIsStoredInMemory(newUuid());
        DataSourceMetaData metaData = dataSourceWrapper.metaData();

        assertThat(metaData.productName())
                .isEqualTo(H2_1_4.getDatabaseProductName());
        assertThat(metaData.majorVersion())
                .isEqualTo(H2_1_4.getMajorVersion());
        assertThat(metaData.minorVersion())
                .isEqualTo(H2_1_4.getMinorVersion());
    }

    @Test
    @MuteLogging
    @DisplayName("throw `DatabaseException` if failed to obtain a meta data")
    void throwOnGetMetaDataError() {
        ThrowingHikariDataSource dataSource = whichIsThrowingByCommand(newUuid());
        DataSourceWrapper wrapper = DataSourceWrapper.wrap(dataSource);
        dataSource.setThrowOnGetConnection(true);

        assertThrows(DatabaseException.class, wrapper::metaData);
    }

    @Test
    @DisplayName("throw ISE if obtaining meta data when already closed")
    void throwIseIfAlreadyClosed() {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        dataSource.close();

        assertThrows(IllegalStateException.class, dataSource::metaData);
    }
}
