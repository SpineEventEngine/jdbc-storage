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

package io.spine.server.storage.jdbc.operation.mysql;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.record.NewRecordTable;
import io.spine.server.storage.jdbc.operation.WriteOne;

/**
 * A MySQL-optimized operation for writing a single record into the database.
 *
 * //TODO:2021-06-24:alex.tymchenko: document once implemented.
 *
 * @param <I>
 *         the type of the identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
public final class MysqlWriteOne<I, R extends Message> extends WriteOne<I, R> {

    /**
     * Creates a new operation.
     *
     * @param table
     *         a table to write into
     * @param dataSource
     *         data source to use
     */
    public MysqlWriteOne(NewRecordTable<I, R> table, DataSourceWrapper dataSource) {
        super(table, dataSource);
    }

    //INSERT..ON DUPLICATE UPDATE
//    @Override
//    public void execute(JdbcRecord<I, R> record) {
//        super.execute(record);
//    }
}
