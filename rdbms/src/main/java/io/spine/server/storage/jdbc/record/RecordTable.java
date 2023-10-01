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

    /**
     * Creates a new instance of record table by the passed specifications.
     *
     * @param spec
     *         table specifications to use
     * @param factory
     *         storage factory to obtain the connection settings from
     * @param <I>
     *         the type of identifiers of stored records
     * @param <R>
     *         the type of stored records
     * @return a new instance of this type
     */
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

    /**
     * Creates a table in the underlying storage.
     */
    public void create() {
        operations.createTable(this)
                  .execute();
    }

    /**
     * Reads the identifiers of the records which match the passed query,
     * and returns an iterator over the results.
     */
    public Iterator<I> index(RecordQuery<I, R> query) {
        var records = read(query);
        var ids = Iterators.transform(records, spec::idFromRecord);
        return ids;
    }

    /**
     * Returns a new iterator over the identifiers of the records,
     * stored in the underlying table.
     */
    public Iterator<I> index() {
        var result = operations.index(this)
                               .execute();
        return result;
    }

    /**
     * Writes the record into this table.
     *
     * <p>This operation may be internally adjusted by the framework
     * to the underlying RDBMS engine,
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

    /**
     * Reads records matching the passed query,
     * and returns an iterator over the results.
     */
    public Iterator<R> read(RecordQuery<I, R> query) {
        var result = operations.readManyByQuery(this)
                               .execute(query);
        return result;
    }

    /**
     * Deletes the record with the specified identifier
     * from the underlying storage.
     *
     * <p>Returns {@code true}, if there was a record deleted,
     * {@code false} otherwise.
     */
    public boolean delete(I id) {
        var result = operations.deleteOne(this)
                               .execute(id);
        return result;
    }

    /**
     * Deletes multiple records by the passed identifiers.
     */
    public void deleteMany(Iterable<I> ids) {
        operations.deleteManyByIds(this)
                  .execute(ids);
    }

    /**
     * Writes multiple records to the underlying storage.
     */
    public void writeAll(Iterable<? extends RecordWithColumns<I, R>> records) {
        Iterable<JdbcRecord<I, R>> transformed =
                StreamSupport.stream(records.spliterator(), false)
                        .map(r -> new JdbcRecord<>(spec, r))
                        .collect(Collectors.toList());
        operations.writeBulk(this)
                  .execute(transformed);
    }

    /**
     * Returns the data source, on top of which this table is created.
     */
    protected final DataSourceWrapper dataSource() {
        return dataSource;
    }
}
