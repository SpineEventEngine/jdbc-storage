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
 * @author Dmytro Dashenkov.
 */
public interface VisibilityHandlingStorageQueryFactory<I> {

    CreateVisibilityTableQuery newCreateVisibilityTableQuery();

    InsertVisibilityQuery newInsertVisibilityQuery(I id, Visibility entityStatus);

    SelectVisibilityQuery newSelectVisibilityQuery(I id);

    UpdateVisibilityQuery newUpdateVisibilityQuery(I id, Visibility status);

    MarkEntityQuery<I> newMarkArchivedQuery(I id);

    MarkEntityQuery<I> newMarkDeletedQuery(I id);

    InsertAndMarkEntityQuery<I> newMarkArchivedNewEntityQuery(I id);

    InsertAndMarkEntityQuery<I> newMarkDeletedNewEntityQuery(I id);

    void setLogger(Logger logger);
}
