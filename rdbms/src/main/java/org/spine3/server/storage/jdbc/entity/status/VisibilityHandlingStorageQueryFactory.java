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

package org.spine3.server.storage.jdbc.entity.status;

import org.slf4j.Logger;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.entity.query.InsertAndMarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.status.query.CreateVisibilityTableQuery;
import org.spine3.server.storage.jdbc.entity.status.query.InsertVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.status.query.SelectVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.status.query.UpdateVisibilityQuery;

/**
 * And interface for managing a table for storing the {@linkplain Visibility entity visibility}.
 *
 * @param <I> ID type of the entity visibility of which is stored
 * @author Dmytro Dashenkov.
 */
public interface VisibilityHandlingStorageQueryFactory<I> {

    /**
     * Generates a query for creating a new table of given name for storing
     * the {@linkplain Visibility entity visibility} if the table does not exist yet.
     *
     * @return a {@code CREATE TABLE IF NOT EXISTS} query
     */
    CreateVisibilityTableQuery newCreateVisibilityTableQuery();

    /**
     * Generates a query for inserting a new record of
     * the {@linkplain Visibility entity visibility}.
     *
     * @param id         ID of the new record
     * @param visibility the {@linkplain Visibility} to store
     * @return an {@code INSERT INTO} query
     */
    InsertVisibilityQuery newInsertVisibilityQuery(I id, Visibility visibility);

    /**
     * Generates a query for selecting the {@linkplain Visibility} by ID.
     *
     * @param id ID to look for
     * @return a {@code SELECT} query
     */
    SelectVisibilityQuery newSelectVisibilityQuery(I id);

    /**
     * Generates a query for updating an existing record of
     * the {@linkplain Visibility entity visibility}.
     *
     * @param id         ID of the record
     * @param visibility new {@linkplain Visibility} to store
     * @return an {@code UPDATE} query
     */
    UpdateVisibilityQuery newUpdateVisibilityQuery(I id, Visibility visibility);

    /**
     * Generates a new query setting an existing {@linkplain Visibility} with the passed ID to
     * {@code archived}.
     *
     * @param id ID of an existing record
     * @return an {@code UPDATE} query
     */
    MarkEntityQuery<I> newMarkArchivedQuery(I id);

    /**
     * Generates a new query setting an existing {@linkplain Visibility} with the passed ID to
     * {@code deleted}.
     *
     * @param id ID of an existing record
     * @return an {@code UPDATE} query
     */
    MarkEntityQuery<I> newMarkDeletedQuery(I id);

    /**
     * Generates a new query creating a new {@linkplain Visibility} record with the passed ID and
     * set to {@code archived}.
     *
     * @param id ID of the new record
     * @return an {@code INSERT INTO} query
     */
    InsertAndMarkEntityQuery<I> newMarkArchivedNewEntityQuery(I id);

    /**
     * Generates a new query creating a new {@linkplain Visibility} record with the passed ID and
     * set to {@code deleted}.
     *
     * @param id ID of the new record
     * @return an {@code INSERT INTO} query
     */
    InsertAndMarkEntityQuery<I> newMarkDeletedNewEntityQuery(I id);

    /**
     * Sets the {@link Logger} to use in the generated queries.
     */
    void setLogger(Logger logger);
}
