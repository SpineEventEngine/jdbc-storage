/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record;

import com.google.common.collect.Iterators;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.logging.WithLogging;
import io.spine.query.RecordQuery;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.operation.OperationFactory;
import io.spine.server.storage.jdbc.record.column.IdColumn;

import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An SQL table storing a single {@link Message} type.
 *
 * @param <I>
 *         the type of identifiers of stored records
 * @param <R>
 *         the type of stored records
 */
public class RecordTable<I, R extends Message> implements WithLogging {

    private final JdbcTableSpec<I, R> spec;
    private final OperationFactory operations;
    private final Descriptor descriptor;
    private final DataSourceWrapper dataSource;

    protected RecordTable(JdbcTableSpec<I, R> tableSpec, JdbcStorageFactory factory) {
        spec = tableSpec;
        operations = factory.operations();
        dataSource = factory.dataSource();
        this.descriptor = tableSpec.recordDescriptor();
    }

    public static <I, R extends Message> RecordTable<I, R>
    by(JdbcTableSpec<I, R> spec, JdbcStorageFactory factory) {
        checkNotNull(spec);
        checkNotNull(factory);
        return new RecordTable<>(spec, factory);
    }

    /**
     * Returns the definition of the ID column for this table.
     */
    public final IdColumn<I> idColumn() {
        return spec.idColumn();
    }

    /**
     * Returns the table name.
     */
    public final String name() {
        return spec.tableName();
    }

    /**
     * Returns the Protobuf descriptor corresponding to the stored record.
     */
    public final Descriptor descriptor() {
        return descriptor;
    }

    /**
     * Returns the storage specification for this table.
     */
    public final JdbcTableSpec<I, R> spec() {
        return spec;
    }

    public void create() {
        operations.createTable(this)
                  .execute();
    }

    //TODO:2021-06-24:alex.tymchenko: think of introducing a separate operation for this.
    public Iterator<I> index(RecordQuery<I, R> query) {
        var records = read(query);
        var ids = Iterators.transform(records, spec::idFromRecord);
        return ids;
    }

    public Iterator<I> index() {
        var result = operations.index(this)
                               .execute();
        return result;
    }

    /**
     * Writes the record into this table.
     *
     * <p>This operation may be adjusted to the underlying RDBMS engine,
     * including usage of RDBMS-specific SQL expressions.
     *
     * @param record
     *         record to write
     */
    public void write(RecordWithColumns<I, R> record) {
        var operation = operations.writeOne(this);
        var wrapped = new JdbcRecord<>(spec, record);
        operation.execute(wrapped);
    }

//    /**
//     * Inserts the record into the table using the specified ID.
//     *
//     * @param record
//     *         a record to insert
//     */
    //TODO:2021-06-24:alex.tymchenko: do we need these methods?
//    public void insert(RecordWithColumns<I, R> record) {
//        WriteOperation<I, R> operation = operationFactory.insertOne(this);
//        operation.execute(record);
//        WriteQuery query = newInsert(record);
//        query.execute();
//    }
//
//    /**
//     * Updates the record with the specified ID for the table.
//     *
//     * @param record
//     *         a new state of the record
//     */
//    private void update(RecordWithColumns<I, R> record) {
//        WriteQuery query = newUpdateQuery(record);
//        query.execute();
//    }

    public Iterator<R> read(RecordQuery<I, R> query) {
        var result = operations.readManyByQuery(this)
                               .execute(query);
        return result;
    }

    public boolean delete(I id) {
        var result = operations.deleteOne(this)
                               .execute(id);
        return result;
    }

    public void deleteMany(Iterable<I> ids) {
        operations.deleteManyByIds(this)
                  .execute(ids);
    }

    public void writeAll(Iterable<? extends RecordWithColumns<I, R>> records) {
        Iterable<JdbcRecord<I, R>> transformed =
                StreamSupport.stream(records.spliterator(), false)
                        .map(r -> new JdbcRecord<>(spec, r))
                        .collect(Collectors.toList());
        operations.writeBulk(this)
                  .execute(transformed);
    }

    /**
     * Obtains multiple messages from the table by IDs.
     *
     * <p>The non-existent IDs are ignored.
     */
    protected Iterator<R> readAll(Iterable<I> ids) {
        var result = operations.readManyByIds(this)
                               .execute(ids);
        return result;
    }

    protected final DataSourceWrapper dataSource() {
        return dataSource;
    }
}
