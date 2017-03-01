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

package org.spine3.server.storage.jdbc.entity.visibility;

import com.google.common.base.Optional;
import com.google.protobuf.Message;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.entity.visibility.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.SelectVisibilityQuery;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.validate.Validate.isDefault;

/**
 * A helper component for handling operations with {@linkplain Visibility entity visibility}.
 *
 * <p>To store the info about the {@linkplain Visibility entity visibility}
 * {@code VisibilityHandler} uses a separate table.
 *
 * @param <I> ID type of the entity visibility of which is stored
 * @author Dmytro Dashenkov
 */
public class VisibilityHandler<I> {

    private final VisibilityHandlingStorageQueryFactory<I> queryFactory;

    /**
     * Creates a new instance of the {@code VisibilityHandler}
     *
     * @param queryFactory a factory for generating the queries to the table in which the visibility
     *                     is stored
     */
    public VisibilityHandler(VisibilityHandlingStorageQueryFactory<I> queryFactory) {
        this.queryFactory = checkNotNull(queryFactory);
    }

    /**
     * Creates the table if it doesn't exist yet.
     *
     * <p>SQL equivalent of calling this method is:
     * <pre>
     *   <code>
     *     CREATE TABLE IF NOT EXISTS %table-name% (
     *         id VARCHAR(512),
     *         archived BOOLEAN,
     *         deleted BOOLEAN
     *     );
     *   </code>
     * </pre>
     *
     * <p>Common usage is to call this method right after the constructor, but this is left
     * on behalf of the class user.
     */
    public void initialize() {
        queryFactory.newCreateVisibilityTableQuery()
                    .execute();
    }

    /**
     * Reads the {@linkplain Visibility} from the database.
     *
     * @param id ID of the record
     * @return {@code Optional.absent()} if the {@linkplain Visibility} was not found or was
     * {@linkplain org.spine3.validate.Validate#isDefault(Message) default},
     * {@code Optional.of)} otherwise
     * the record otherwise
     */
    public Optional<Visibility> readVisibility(I id) {
        final SelectVisibilityQuery query = queryFactory.newSelectVisibilityQuery(id);
        final Visibility visibility = query.execute();
        final boolean absent = visibility == null || isDefault(visibility);
        if (absent) {
            return Optional.absent();
        }
        return Optional.of(visibility);
    }

    /**
     * Inserts a new {@linkplain Visibility} into the database or updates an existing one.
     *
     * @param id         ID of the record to store
     * @param visibility the {@linkplain Visibility} to store
     */
    public void writeVisibility(I id, Visibility visibility) {
        final Optional<Visibility> current = readVisibility(id);
        final boolean exists = current.isPresent();
        writeVisibility(id, visibility, exists);
    }

    private void writeVisibility(I id, Visibility status, boolean updateExisting) {
        if (updateExisting) {
            updateVisibility(id, status);
        } else {
            insertVisibility(id, status);
        }
    }

    private void updateVisibility(I id, Visibility status) {
        queryFactory.newUpdateVisibilityQuery(id, status)
                    .execute();
    }

    private void insertVisibility(I id, Visibility status) {
        queryFactory.newInsertVisibilityQuery(id, status)
                    .execute();
    }

    /**
     * Creates a new record as archived or updates an existing one to become archived.
     *
     * <p>Has no effect if the record exists and has been marked archived previously.
     *
     * @param id ID of the record to store
     */
    public void markArchived(I id) {
        final MarkEntityQuery query;
        if (!containsRecord(id)) {
            query = queryFactory.newMarkArchivedNewEntityQuery(id);
        } else {
            query = queryFactory.newMarkArchivedQuery(id);
        }
        query.execute();
    }

    /**
     * Creates a new record as deleted or updates an existing one to become deleted.
     *
     * <p>Has no effect if the record exists and has been marked deleted previously.
     *
     * @param id ID of the record to store
     */
    public void markDeleted(I id) {
        final MarkEntityQuery query;
        if (!containsRecord(id)) {
            query = queryFactory.newMarkDeletedNewEntityQuery(id);
        } else {
            query = queryFactory.newMarkDeletedQuery(id);
        }
        query.execute();
    }

    private boolean containsRecord(I id) {
        final SelectVisibilityQuery selectQuery = queryFactory.newSelectVisibilityQuery(id);
        final Visibility visibility = selectQuery.execute();
        return visibility != null;
    }
}
