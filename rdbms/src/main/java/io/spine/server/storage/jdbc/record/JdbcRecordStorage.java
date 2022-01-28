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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import io.spine.query.RecordQuery;
import io.spine.server.ContextSpec;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.jdbc.JdbcStorageFactory;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A storage which stores Protobuf message records in a single RDBMS {@linkplain RecordTable table}
 * accessed via JDBC connection.
 *
 * @param <I>
 *         the type of identifiers of stored records
 * @param <R>
 *         the type of stored records
 */
public class JdbcRecordStorage<I, R extends Message> extends RecordStorage<I, R> {

    private final RecordTable<I, R> table;

    public JdbcRecordStorage(ContextSpec contextSpec,
                             RecordSpec<I, R, ?> recordSpec,
                             JdbcStorageFactory factory) {
        super(contextSpec, recordSpec);
        var tableSpec = factory.tableSpecFor(recordSpec);
        this.table = RecordTable.by(tableSpec, factory);
        this.table.create();
    }

    @Override
    protected Iterator<I> index(RecordQuery<I, R> query) {
        return table.index(query);
    }

    @Override
    protected void writeRecord(RecordWithColumns<I, R> record) {
        checkNotNull(record);
        table.write(record);
    }

    @Override
    protected void writeAllRecords(Iterable<? extends RecordWithColumns<I, R>> records) {
        checkNotNull(records);
        checkNotClosed();
        table.writeAll(records);
    }

    @Override
    protected Iterator<R> readAllRecords(RecordQuery<I, R> query) {
        return table.read(query);
    }

    @CanIgnoreReturnValue
    @Override
    protected boolean deleteRecord(I id) {
        return table.delete(id);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Removes all the records by their identifiers in a single call.
     *
     * @implNote The default implementation of this feature provided by {@link
     *         RecordStorage} ensures the storage is not closed, and then removes the records
     *         one-by-one. This method overrides the default behavior in order to execute
     *         the operation in a single take. Therefore, it also double-checks that the storage
     *         is not closed.
     */
    @Override
    protected void deleteAll(Iterable<I> ids) {
        checkNotClosed();
        table.deleteMany(ids);
    }

    @Override
    public Iterator<I> index() {
        return table.index();
    }

    @Override
    public void write(I id, R record) {
        writeRecord(RecordWithColumns.of(id, record));
    }
}
