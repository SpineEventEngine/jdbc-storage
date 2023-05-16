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

package io.spine.server.storage.jdbc.query.given;

import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.given.table.TimestampByString;
import io.spine.server.storage.jdbc.query.ColumnReader;
import io.spine.server.storage.jdbc.query.DbIterator;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;

public class DbIteratorTestEnv {

    /** Prevents instantiation of this utility class. */
    private DbIteratorTestEnv() {
    }

    public static DbIterator emptyIterator() {
        ResultSet resultSet = emptyResultSet();
        DbIterator iterator = anIterator(resultSet);
        return iterator;
    }

    /**
     * An iterator which produces failure on every operation except {@link DbIterator#close()}, as
     * it's using a closed {@link ResultSet}.
     */
    public static DbIterator faultyResultIterator() throws SQLException {
        ResultSet resultSet = closedResultSet();
        DbIterator iterator = anIterator(resultSet);
        return iterator;
    }

    public static DbIterator sneakyResultIterator() {
        ResultSet resultSet = resultSetWithSingleResult();
        DbIterator iterator = throwingIterator(resultSet);
        return iterator;
    }

    public static DbIterator nonEmptyIterator() {
        ResultSet resultSet = resultSetWithSingleResult();
        DbIterator iterator = anIterator(resultSet);
        return iterator;
    }

    private static ResultSet emptyResultSet() {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        TimestampByString table = new TimestampByString(dataSource, H2_2_1);
        table.create();

        ResultSet resultSet = table.resultSet(newUuid());
        return resultSet;
    }

    private static ResultSet closedResultSet() throws SQLException {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        TimestampByString table = new TimestampByString(dataSource, H2_2_1);
        table.create();

        ResultSet resultSet = table.resultSet(newUuid());
        resultSet.close();
        return resultSet;
    }

    private static ResultSet resultSetWithSingleResult() {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        TimestampByString table = new TimestampByString(dataSource, H2_2_1);
        table.create();

        Timestamp timestamp = Timestamp
                .newBuilder()
                .setSeconds(142)
                .setNanos(15)
                .build();
        table.write(timestamp);

        String id = table.idOf(timestamp);
        ResultSet resultSet = table.resultSet(id);
        return resultSet;
    }

    private static DbIterator anIterator(ResultSet resultSet) {
        ColumnReader<ResultSet> identityReader = new ColumnReader<ResultSet>("") {
            @Override
            public ResultSet readValue(ResultSet resultSet) {
                return resultSet;
            }
        };
        DbIterator<ResultSet> result = DbIterator.over(resultSet, identityReader);
        return result;
    }

    private static DbIterator throwingIterator(ResultSet resultSet) {
        ColumnReader<ResultSet> throwingReader = new ColumnReader<ResultSet>("") {
            @Override
            public ResultSet readValue(ResultSet resultSet) throws SQLException {
                throw new SQLException("Read is not allowed; I'm sneaky");
            }
        };
        DbIterator<ResultSet> result = DbIterator.over(resultSet, throwingReader);
        return result;
    }
}
