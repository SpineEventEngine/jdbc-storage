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

package org.spine3.server.storage.jdbc.entity.visibility;

import org.junit.Test;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.Tests;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmytro Dashenkov.
 */
public class VisibilityQueryFactoriesShould {

    @Test
    public void have_private_utility_constructor() {
        assertTrue(Tests.hasPrivateParameterlessCtor(VisibilityQueryFactories.class));
    }

    @Test
    public void create_factory_on_separate_table() {
        // No matter which data source is used
        final VisibilityHandlingStorageQueryFactory<?> factory =
                VisibilityQueryFactories.forSeparateTable(
                        DataSourceWrapper.wrap(GivenDataSource.whichIsAutoCloseable()));
        assertThat(factory, instanceOf(VisibilityHandlingStorageQueryFactoryImpl.class));
        final String tableName = ((VisibilityHandlingStorageQueryFactoryImpl) factory).getTableName();
        assertEquals(VisibilityTable.TABLE_NAME, tableName);
    }
}
