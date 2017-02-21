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
import org.spine3.server.entity.status.EntityStatus;
import org.spine3.server.storage.jdbc.entity.status.query.SelectEntityStatusQuery;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.validate.Validate.isDefault;

/**
 * @author Dmytro Dashenkov.
 */
public class EntityStatusHandler<I> {

    private final EntityStatusHandlingStorageQueryFactory<I> queryFactory;

    public EntityStatusHandler(EntityStatusHandlingStorageQueryFactory<I> queryFactory) {
        this.queryFactory = checkNotNull(queryFactory);
    }

    public void initialize() {
        queryFactory.newCreateEntityStatusTableQuery()
                    .execute();
    }

    public Optional<EntityStatus> readStatus(I id) {
        final SelectEntityStatusQuery query = queryFactory.newSelectEntityStatusQuery(id);
        final EntityStatus status = query.execute();
        final boolean absent = isDefault(status);
        if (absent) {
            return Optional.absent();
        }
        return Optional.of(status);
    }

    public void writeStatus(I id, EntityStatus status) {
        final Optional<EntityStatus> currentStatus = readStatus(id);
        final boolean exists = currentStatus.isPresent();
        writeStatus(id, status, exists);
    }

    private void writeStatus(I id, EntityStatus status, boolean updateExisting) {
        if (updateExisting) {
            updateStatus(id, status);
        } else {
            insertStatus(id, status);
        }
    }

    private void updateStatus(I id, EntityStatus status) {
        queryFactory.newUpdateEntityStatusQuery(id, status)
                    .execute();
    }

    private void insertStatus(I id, EntityStatus status) {
        queryFactory.newInsertEntityStatusQuery(id, status)
                    .execute();
    }

    public boolean markArchived(I id) {
        final boolean result = queryFactory.newMarkArchivedQuery(id).execute();
        return result;
    }

    public boolean markDeleted(I id) {
        final boolean result = queryFactory.newMarkDeletedQuery(id).execute();
        return result;
    }
}
