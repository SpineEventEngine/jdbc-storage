/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import io.spine.server.storage.jdbc.DatabaseException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.emptyIterator;
import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.faultyResultIterator;
import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.nonEmptyIterator;
import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.sneakyResultIterator;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"InnerClassMayBeStatic", "ClassCanBeStatic"})
// JUnit nested classes cannot be static.
@DisplayName("DbIterator should")
class DbIteratorTest {

    @SuppressWarnings("NonExceptionNameEndsWithException") // For test name clarity.
    @Nested
    @DisplayName("throw DatabaseException")
    class ThrowDatabaseException {

        @Test
        @DisplayName("on `hasNext` check failure")
        void onNextCheckFailure() throws SQLException {
            DbIterator iterator = faultyResultIterator();
            assertThrows(DatabaseException.class, iterator::hasNext);
        }

        @Test
        @DisplayName("on read failure")
        void onReadFailure() {
            DbIterator iterator = sneakyResultIterator();
            assertThrows(DatabaseException.class, () -> {
                if (iterator.hasNext()) {
                    iterator.next();
                }
            });
        }
    }

    @SuppressWarnings("CheckReturnValue") // Just check that method runs without errors.
    @Test
    @DisplayName("allow `next` without `hasNext`")
    void allowNextWithoutHasNext() {
        DbIterator iterator = nonEmptyIterator();
        iterator.next();
    }

    @Nested
    @DisplayName("close ResultSet")
    class CloseResultSet {

        @Test
        @DisplayName("when told to do so")
        void whenTold() throws SQLException {
            DbIterator iterator = nonEmptyIterator();
            iterator.close();

            assertClosed(iterator);
        }

        @SuppressWarnings("CheckReturnValue") // Call `hasNext` method just to close iterator.
        @Test
        @DisplayName("when no more elements are present to iterate")
        void whenNoElementsPresent() throws SQLException {
            DbIterator iterator = emptyIterator();
            iterator.hasNext();

            assertClosed(iterator);
        }
    }

    @SuppressWarnings("deprecation") // Use deprecated method to make sure it's not supported.
    @Test
    @DisplayName("not support removal")
    void notSupportRemoval() {
        DbIterator iterator = emptyIterator();
        assertThrows(UnsupportedOperationException.class, iterator::remove);
    }

    @SuppressWarnings("CheckReturnValue") // Ignore `hasNext` method result on purpose.
    @Test
    @DisplayName("throw NoSuchElementException if trying to get absent element")
    void notGetAbsentElement() {
        DbIterator iterator = emptyIterator();

        // Ignore that the element is absent.
        iterator.hasNext();

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    @DisplayName("do nothing on close if result set is already closed")
    void doNothingIfAlreadyClosed() throws SQLException {
        DbIterator iterator = emptyIterator();

        ResultSet resultSet = iterator.resultSet();
        resultSet.close();
        assertThat(resultSet.isClosed())
                .isTrue();

        iterator.close();
        assertClosed(iterator);
    }

    private static void assertClosed(DbIterator<?> iterator) throws SQLException {
        ResultSet resultSet = iterator.resultSet();
        assertThat(resultSet.isClosed())
                .isTrue();
    }
}
