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

package io.spine.server.storage.jdbc.query.dsl;

import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;

/**
 * A factory of common write queries for all tables.
 *
 * @author Dmytro Grankin
 */
abstract class AbstractWriteQueryFactory <I, R> implements WriteQueryFactory<I, R> {

    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final String tableName;

    /**
     * Creates a new instance.
     *
     * @param idColumn the {@link IdColumn} for working with IDs of this factory
     * @param dataSource instance of {@link DataSourceWrapper}
     * @param tableName  the name of the table to generate queries for
     */
    AbstractWriteQueryFactory(IdColumn<I> idColumn, DataSourceWrapper dataSource, String tableName) {
        this.idColumn = idColumn;
        this.dataSource = dataSource;
        this.tableName = tableName;
    }

    @Override
    public WriteQuery newDeleteQuery(I id) {
        final DeleteRecordQuery.Builder<I> builder = DeleteRecordQuery.newBuilder();
        final DeleteRecordQuery<I> query = builder.setTableName(tableName)
                                                  .setIdColumn(getIdColumn())
                                                  .setIdValue(id)
                                                  .setDataSource(dataSource)
                                                  .build();
        return query;
    }

    @Override
    public WriteQuery newDeleteAllQuery() {
        final DeleteAllQuery query = DeleteAllQuery.newBuilder()
                                                   .setTableName(tableName)
                                                   .setDataSource(dataSource)
                                                   .build();
        return query;
    }

    IdColumn<I> getIdColumn() {
        return idColumn;
    }

    DataSourceWrapper getDataSource() {
        return dataSource;
    }

    String getTableName() {
        return tableName;
    }
}
