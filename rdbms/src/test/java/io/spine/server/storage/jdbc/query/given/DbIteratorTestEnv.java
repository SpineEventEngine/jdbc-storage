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

package io.spine.server.storage.jdbc.query.given;

import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.PredefinedMapping;
import io.spine.server.storage.jdbc.given.table.TimestampByString;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.reader.ColumnReader;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;

public class DbIteratorTestEnv {

    /** Prevents instantiation of this utility class. */
    private DbIteratorTestEnv() {
    }

    public static DbIterator<?> emptyIterator() {
        var resultSet = emptyResultSet();
        var iterator = anIterator(resultSet);
        return iterator;
    }

    /**
     * An iterator which produces failure on every operation except {@link DbIterator#close()}, as
     * it's using a closed {@link ResultSet}.
     */
    public static DbIterator<?> faultyResultIterator() throws SQLException {
        var resultSet = closedResultSet();
        var iterator = anIterator(resultSet);
        return iterator;
    }

    public static DbIterator<?> sneakyResultIterator() {
        var resultSet = resultSetWithSingleResult();
        var iterator = throwingIterator(resultSet);
        return iterator;
    }

    public static DbIterator<?> nonEmptyIterator() {
        var resultSet = resultSetWithSingleResult();
        var iterator = anIterator(resultSet);
        return iterator;
    }

    private static ResultSet emptyResultSet() {
        var table = table();
        table.create();

        var resultSet = table.resultSet(newUuid());
        return resultSet;
    }

    private static ResultSet closedResultSet() throws SQLException {
        var table = table();
        table.create();

        var resultSet = table.resultSet(newUuid());
        resultSet.close();
        return resultSet;
    }

    private static ResultSet resultSetWithSingleResult() {
        var table = table();
        table.create();

        var timestamp = Timestamp
                .newBuilder()
                .setSeconds(142)
                .setNanos(15)
                .build();
        table.write(timestamp);

        var id = table.idOf(timestamp);
        var resultSet = table.resultSet(id);
        return resultSet;
    }

    private static TimestampByString table() {
        var factory = inMemoryFactory(H2_2_1);
        var table = new TimestampByString(factory);
        return table;
    }

    private static JdbcStorageFactory inMemoryFactory(PredefinedMapping typeMapping) {
        return JdbcStorageFactory.newBuilder()
                .setTypeMapping(typeMapping)
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .build();
    }

    private static DbIterator<?> anIterator(ResultSet resultSet) {
        var identityReader = new ColumnReader<ResultSet>("") {
            @Override
            public ResultSet readValue(ResultSet resultSet) {
                return resultSet;
            }
        };
        var result = DbIterator.over(resultSet, identityReader);
        return result;
    }

    private static DbIterator<?> throwingIterator(ResultSet resultSet) {
        var throwingReader = new ColumnReader<ResultSet>("") {
            @Override
            public ResultSet readValue(ResultSet resultSet) throws SQLException {
                throw new SQLException("Read is not allowed; I'm sneaky");
            }
        };
        var result = DbIterator.over(resultSet, throwingReader);
        return result;
    }
}
