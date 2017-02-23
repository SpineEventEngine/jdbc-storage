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

import com.google.common.base.Optional;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.entity.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.status.query.SelectVisibilityQuery;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.validate.Validate.isDefault;

/**
 * @author Dmytro Dashenkov.
 */
public class VisibilityHandler<I> {

    private final VisibilityHandlingStorageQueryFactory<I> queryFactory;

    public VisibilityHandler(VisibilityHandlingStorageQueryFactory<I> queryFactory) {
        this.queryFactory = checkNotNull(queryFactory);
    }

    public void initialize() {
        queryFactory.newCreateVisibilityTableQuery()
                    .execute();
    }

    public Optional<Visibility> readStatus(I id) {
        final SelectVisibilityQuery query = queryFactory.newSelectVisibilityQuery(id);
        final Visibility status = query.execute();
        final boolean absent = status == null || isDefault(status);
        if (absent) {
            return Optional.absent();
        }
        return Optional.of(status);
    }

    public void writeStatus(I id, Visibility status) {
        final Optional<Visibility> currentStatus = readStatus(id);
        final boolean exists = currentStatus.isPresent();
        writeStatus(id, status, exists);
    }

    private void writeStatus(I id, Visibility status, boolean updateExisting) {
        if (updateExisting) {
            updateStatus(id, status);
        } else {
            insertStatus(id, status);
        }
    }

    private void updateStatus(I id, Visibility status) {
        queryFactory.newUpdateVisibilityQuery(id, status)
                    .execute();
    }

    private void insertStatus(I id, Visibility status) {
        queryFactory.newInsertVisibilityQuery(id, status)
                    .execute();
    }

    public void markArchived(I id) {
        final MarkEntityQuery query;
        if (!containsRecord(id)) {
            query = queryFactory.newInsertAndMarkArchivedEntityQuery(id);
        } else {
            query = queryFactory.newMarkArchivedQuery(id);
        }
        query.execute();
    }

    public void markDeleted(I id) {
        final MarkEntityQuery query;
        if (!containsRecord(id)) {
            query = queryFactory.newInsertAndMarkDeletedEntityQuery(id);
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
