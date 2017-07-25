/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

import io.spine.server.storage.jdbc.table.AbstractTable;

/**
 * An interface of a {@linkplain StorageQuery query} factory for the read queries.
 *
 * <p>Each JDBC {@linkplain AbstractTable table} uses a query factory to construct its SQL queries
 * to the database.
 *
 * @param <I> type of the ID of the record
 * @param <R> type of the record
 *
 * @author Dmytro Dashenkov
 * @see WriteQueryFactory
 */
public interface ReadQueryFactory<I, R> {

    /**
     * Creates an {@linkplain SelectByIdQuery read by ID query}.
     *
     * @return a query for selecting a record by given ID
     */
    SelectByIdQuery<I, R> newSelectByIdQuery(I id);

    /**
     * Creates an {@linkplain StorageIndexQuery index query} for the given table.
     *
     * @return a query retrieving all the IDs of a table
     */
    StorageIndexQuery<I> newIndexQuery();
}
