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

import org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

/**
 * A utility helping to initialize a {@link VisibilityHandlingStorageQueryFactory} with the required
 * configuration.
 *
 * @author Dmytro Dashenkov.
 */
public class VisibilityQueryFactories {

    private VisibilityQueryFactories() {
    }

    /**
     * Creates an instance of {@link VisibilityHandlingStorageQueryFactory} which stores its data
     * in the given table.
     *
     * @param dataSource {@linkplain DataSourceWrapper} to use
     * @param tableName  the name of the table to use
     * @param idColumn   the {@linkplain IdColumn column} of the table which stores its IDs
     * @param <I>        the Java type of the ID
     * @return new instance of the query factory
     */
    public static <I> VisibilityHandlingStorageQueryFactory<I> forTable(
            DataSourceWrapper dataSource,
            String tableName,
            IdColumn<I> idColumn) {
        return new VisibilityHandlingStorageQueryFactoryImpl<>(dataSource,
                                                               tableName,
                                                               idColumn);
    }

    /**
     * Creates an instance of {@link VisibilityHandlingStorageQueryFactory} which stores its data
     * in a separate table.
     *
     * @param dataSource {@linkplain DataSourceWrapper} to use
     * @return new instance of the query factory
     */
    public static VisibilityHandlingStorageQueryFactory<?> forSeparateTable(
            DataSourceWrapper dataSource) {
        return new VisibilityHandlingStorageQueryFactoryImpl<>(dataSource,
                                                               VisibilityTable.TABLE_NAME,
                                                               new IdColumn.StringIdColumn());
    }
}
