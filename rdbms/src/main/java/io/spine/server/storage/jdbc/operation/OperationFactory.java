/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.operation;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.server.storage.jdbc.operation.mysql.MysqlWriteBulk;
import io.spine.server.storage.jdbc.operation.mysql.MysqlWriteOne;
import io.spine.server.storage.jdbc.operation.postgres.PostgresWriteBulk;
import io.spine.server.storage.jdbc.operation.postgres.PostgresWriteOne;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.operation.DetectedEngine.MySQL;
import static io.spine.server.storage.jdbc.operation.DetectedEngine.Postgres;

/**
 * A factory of {@link Operation}s.
 */
public final class OperationFactory {

    private final DataSourceWrapper dataSource;
    private final DetectedEngine engine;
    private final JdbcColumnMapping columnMapping;
    private final TypeMapping typeMapping;

    private OperationFactory(DataSourceWrapper wrapper,
                             JdbcColumnMapping columnMapping,
                             TypeMapping typeMapping) {
        this.dataSource = wrapper;
        this.columnMapping = columnMapping;
        this.typeMapping = typeMapping;
        var metadata = dataSource.metaData();
        engine = DetectedEngine.from(metadata);
    }

    /**
     * Creates a new factory on top of the passed data source.
     */
    public static OperationFactory with(DataSourceWrapper dataSource,
                                        JdbcColumnMapping columnMapping,
                                        TypeMapping typeMapping) {
        checkNotNull(dataSource);
        return new OperationFactory(dataSource, columnMapping, typeMapping);
    }

    public <I, R extends Message> WriteOne<I, R> writeOne(RecordTable<I, R> table) {
        if (engine == MySQL) {
            return new MysqlWriteOne<>(table, dataSource);
        }
        if (engine == Postgres) {
            return new PostgresWriteOne<>(table, dataSource);
        }

        return new WriteOne<>(table, dataSource);
    }

    public <I, R extends Message> WriteBulk<I, R> writeBulk(RecordTable<I, R> table) {
        if (engine == MySQL) {
            return new MysqlWriteBulk<>(table, dataSource, this);
        }
        if (engine == Postgres) {
            return new PostgresWriteBulk<>(table, dataSource, this);
        }

        return new WriteBulk<>(table, dataSource, this);
    }

    public <I, R extends Message> ReadManyByIds<I, R> readManyByIds(RecordTable<I, R> t) {
        return new ReadManyByIds<>(t, dataSource);
    }

    public <I, R extends Message> ReadManyByQuery<I, R> readManyByQuery(RecordTable<I, R> t) {
        return new ReadManyByQuery<>(t, dataSource, columnMapping);
    }

    public <I, R extends Message> DeleteOne<I, R> deleteOne(RecordTable<I, R> t) {
        return new DeleteOne<>(t, dataSource);
    }

    public <I, R extends Message> DeleteAll<I, R> deleteAll(RecordTable<I, R> t) {
        return new DeleteAll<>(t, dataSource);
    }

    public <I, R extends Message> DeleteManyByIds<I, R> deleteManyByIds(RecordTable<I, R> t) {
        return new DeleteManyByIds<>(t, dataSource);
    }

    public <I, R extends Message> CreateTable<I, R> createTable(RecordTable<I, R> t) {
        return new CreateTable<>(t, dataSource, typeMapping);
    }

    public <I, R extends Message> FetchIndex<I, R> index(RecordTable<I, R> t) {
        return new FetchIndex<>(t, dataSource);
    }
}
