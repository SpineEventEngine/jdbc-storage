/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;

import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.emptyIterator;
import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.faultyResultIterator;
import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.nonEmptyIterator;
import static io.spine.server.storage.jdbc.query.given.DbIteratorTestEnv.sneakyResultIterator;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("DbIterator should")
class DbIteratorTest {

    @Test
    @DisplayName("throw DatabaseException on next check failure")
    void throwDatabaseExceptionOnNextCheckFailure() {
        final DbIterator iterator = faultyResultIterator();
        assertThrows(DatabaseException.class, iterator::hasNext);
    }

    @Test
    @DisplayName("throw DatabaseException on close failure")
    void throwDatabaseExceptionOnCloseFailure() {
        final DbIterator iterator = faultyResultIterator();
        assertThrows(DatabaseException.class, iterator::close);
    }

    @Test
    @DisplayName("throw DatabaseException on read failure")
    void throwDatabaseExceptionOnReadFailure() {
        final DbIterator iterator = sneakyResultIterator();
        assertThrows(DatabaseException.class, () -> {
            if (iterator.hasNext()) {
                iterator.next();
            }
        });
    }

    @Test
    @DisplayName("allow `next` without `hasNext`")
    void getNextWithoutHasNext() {
        final DbIterator iterator = nonEmptyIterator();
        iterator.next();
    }

    @Test
    @DisplayName("close ResultSet")
    void closeResultSet() throws SQLException {
        final DbIterator iterator = nonEmptyIterator();
        final ResultSet resultSet = iterator.getResultSet();

        verify(resultSet, never()).close();

        iterator.close();
        assertClosed(iterator);
    }

    @Test
    @DisplayName("close ResultSet if no more elements are present to iterate")
    void closeResultSetWhenEmpty() throws SQLException {
        final DbIterator iterator = emptyIterator();
        final ResultSet resultSet = iterator.getResultSet();

        verify(resultSet, never()).close();

        iterator.hasNext();
        assertClosed(iterator);
    }

    @SuppressWarnings("deprecation") // Need to use deprecated to make sure it's not supported.
    @Test
    @DisplayName("not support removal")
    void notSupportRemoval() {
        final DbIterator iterator = emptyIterator();
        assertThrows(UnsupportedOperationException.class, iterator::remove);
    }

    @Test
    @DisplayName("throw NoSuchElementException if trying to get absent element")
    void throwOnGetAbsentElement() {
        final DbIterator iterator = emptyIterator();

        // Ignore that the element is absent.
        iterator.hasNext();

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    @DisplayName("check if result set is closed")
    void checkIfResultSetClosed() throws SQLException {
        final DbIterator iterator = emptyIterator();
        iterator.close();

        final ResultSet resultSet = iterator.getResultSet();
        verify(resultSet).isClosed();
        verify(resultSet).close();
    }

    @Test
    @DisplayName("obtain statement before result set closed")
    void getStatementBeforeClose() throws SQLException {
        final DbIterator iterator = emptyIterator();
        iterator.close();

        final ResultSet resultSet = iterator.getResultSet();
        final InOrder order = inOrder(resultSet);
        order.verify(resultSet)
             .getStatement();
        order.verify(resultSet)
             .close();
    }

    @Test
    @DisplayName("obtain connection before statement closed")
    void getConnectionBeforeClose() throws SQLException {
        final DbIterator iterator = emptyIterator();
        iterator.close();

        final Statement statement = iterator.getResultSet()
                                            .getStatement();
        final InOrder order = inOrder(statement);
        order.verify(statement)
             .getConnection();
        order.verify(statement)
             .close();
    }

    private static void assertClosed(DbIterator<?> iterator) throws SQLException {
        final ResultSet resultSet = iterator.getResultSet();
        final Statement statement = resultSet.getStatement();
        final Connection connection = statement.getConnection();
        verify(resultSet).close();
        verify(statement).close();
        verify(connection).close();
    }
}