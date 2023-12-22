/*
 * Copyright 2023, TeamDev. All rights reserved.
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
import io.spine.server.storage.jdbc.engine.DetectedEngine;
import io.spine.server.storage.jdbc.engine.PredefinedEngine;
import io.spine.server.storage.jdbc.operation.mysql.MysqlWriteOne;
import io.spine.server.storage.jdbc.record.RecordTable;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.engine.PredefinedEngine.MySQL;

/**
 * A factory of {@link Operation}s.
 *
 * <p>Descendants may extend this type in order to customize the execution of certain operations
 * performed over the underlying database.
 *
 * <p>End-users may choose to use their own operation implementations
 * by extending this type, returning appropriate operation implementations,
 * and using this custom operation factory
 * {@linkplain io.spine.server.storage.jdbc.JdbcStorageFactory.Builder#useOperationFactory(io.spine.server.storage.jdbc.config.CreateOperationFactory)
 *  to build a JdbcStorageFactory}.
 *
 *  <p>Also, all query types are made {@code public} and marked as {@code SPI} elements,
 *  since they may also be involved into customizing the actual SQL queries.
 */
public class OperationFactory {

    private final DataSourceWrapper dataSource;
    private final DetectedEngine engine;
    private final TypeMapping typeMapping;

    /**
     * Creates a new factory on top of the passed data source and the Java-SQL type mapping.
     */
    public OperationFactory(DataSourceWrapper wrapper, TypeMapping mapping) {
        this(wrapper, mapping, detectedEngine(wrapper));
    }

    private static PredefinedEngine detectedEngine(DataSourceWrapper wrapper) {
        var metadata = wrapper.metaData();
        var result = PredefinedEngine.from(metadata);
        return result;
    }

    /**
     * Creates a new factory on top of the passed data source and the Java-SQL type mapping.
     */
    protected OperationFactory(DataSourceWrapper wrapper,
                               TypeMapping mapping,
                               DetectedEngine engine) {
        checkNotNull(wrapper);
        checkNotNull(mapping);
        checkNotNull(engine);
        this.dataSource = wrapper;
        this.typeMapping = mapping;
        this.engine = engine;

    }

    /**
     * Produces an operation which writes a single record to the table.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> WriteOne<I, R> writeOne(RecordTable<I, R> t) {
        if (engine == MySQL) {
            return new MysqlWriteOne<>(t, dataSource);
        }

        return new WriteOne<>(t, dataSource);
    }

    /**
     * Produces an operation which writes several records to the table.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> WriteBulk<I, R> writeBulk(RecordTable<I, R> t) {
        return new WriteBulk<>(t, dataSource, this);
    }

    /**
     * Produces an operation which reads several records from the table by their IDs.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> ReadManyByIds<I, R> readManyByIds(RecordTable<I, R> t) {
        return new ReadManyByIds<>(t, dataSource);
    }

    /**
     * Produces an operation which reads several records from the table by executing a query.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> ReadManyByQuery<I, R> readManyByQuery(RecordTable<I, R> t) {
        return new ReadManyByQuery<>(t, dataSource);
    }

    /**
     * Produces an operation which deletes a single record from the table.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> DeleteOne<I, R> deleteOne(RecordTable<I, R> t) {
        return new DeleteOne<>(t, dataSource);
    }

    /**
     * Produces an operation which deletes several records from the table by their IDs.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> DeleteManyByIds<I, R> deleteManyByIds(RecordTable<I, R> t) {
        return new DeleteManyByIds<>(t, dataSource);
    }

    /**
     * Produces an operation which creates the table in the underlying database.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> CreateTable<I, R> createTable(RecordTable<I, R> t) {
        return new CreateTable<>(t, dataSource, typeMapping);
    }

    /**
     * Produces an operation reads the identifiers of the records stored in the table.
     *
     * @param t
     *         the table to perform the operation over
     * @param <I>
     *         the type of the record identifiers
     * @param <R>
     *         the type of the records stored in the table
     * @return a new operation
     */
    public <I, R extends Message> FetchIndex<I, R> index(RecordTable<I, R> t) {
        return new FetchIndex<>(t, dataSource);
    }

    /**
     * Returns the detected storage engine.
     */
    protected final DetectedEngine engine() {
        return engine;
    }

    /**
     * Returns the data source, against which the created operations
     * are to be performed.
     */
    protected final DataSourceWrapper dataSource() {
        return dataSource;
    }

    /**
     * Returns the Java-SQL type mapping to use within
     * the operations created by this factory.
     */
    protected final TypeMapping typeMapping() {
        return typeMapping;
    }
}
