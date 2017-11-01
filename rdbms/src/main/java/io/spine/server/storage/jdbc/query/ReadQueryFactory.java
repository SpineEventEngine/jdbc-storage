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

import io.spine.annotation.Internal;

import java.util.Iterator;

/**
 * An interface of a {@linkplain StorageQuery query} factory for the read queries.
 *
 * <p>Each JDBC {@linkplain io.spine.server.storage.jdbc.AbstractTable table} uses a query factory
 * to construct its SQL queries to the database.
 *
 * @param <I> type of the ID of the record
 * @param <R> type of the record
 *
 * @author Dmytro Dashenkov
 * @see WriteQueryFactory
 */
@Internal
public interface ReadQueryFactory<I, R> {

    /**
     * Creates an {@linkplain SelectQuery read by ID query}.
     *
     * @return a query for selecting a record by given ID
     */
    SelectQuery<R> newSelectByIdQuery(I id);

    /**
     * Creates an index query for the given table.
     *
     * @return a query retrieving all the IDs of a table
     */
    SelectQuery<Iterator<I>> newIndexQuery();

    /**
     * Creates a {@linkplain SelectQuery contains by ID query}.
     *
     * @param id the ID to check
     * @return a query to check presence of a record
     */
    SelectQuery<Boolean> newContainsQuery(I id);
}
